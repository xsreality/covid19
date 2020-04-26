package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.covid19.TelegramUtils.buildAlertText;
import static org.covid19.TelegramUtils.sendTelegramAlert;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {
    private final KafkaProperties kafkaProperties;
    private Covid19Bot covid19Bot;
    private Serde<String> stringSerde;
    private Serde<PatientAndMessage> patientAndMessageSerde;
    private StateStoresManager storesManager;

    public KafkaStreamsConfig(KafkaProperties kafkaProperties, Covid19Bot covid19Bot, StateStoresManager storesManager) {
        this.kafkaProperties = kafkaProperties;
        this.covid19Bot = covid19Bot;
        this.storesManager = storesManager;
        stringSerde = Serdes.String();
        patientAndMessageSerde = new PatientAndMessageSerde();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> kafkaStreamsProps = new HashMap<>(kafkaProperties.buildStreamsProperties());
        kafkaStreamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaStreamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);
        kafkaStreamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        kafkaStreamsProps.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        return new KafkaStreamsConfiguration(kafkaStreamsProps);
    }

    @Bean
    public KTable<String, String> newsSourcesTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("news-sources",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        Stores.inMemoryKeyValueStore("news-sources-inmemory").name())
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()).withCachingDisabled());
    }

    @Bean
    public KTable<String, StatewiseDelta> dailyStatsTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("statewise-daily-stats",
                Materialized.<String, StatewiseDelta, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("statewise-daily-persistent").name())
                        .withKeySerde(Serdes.String()).withValueSerde(new StatewiseDeltaSerde()).withCachingDisabled());
    }

    @Bean
    public KTable<String, StatewiseDelta> deltaStatsTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("statewise-delta-stats",
                Materialized.<String, StatewiseDelta, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("statewise-delta-persistent").name())
                        .withKeySerde(stringSerde).withValueSerde(new StatewiseDeltaSerde()).withCachingDisabled());
    }

    @Bean
    public KTable<String, UserPrefs> userPrefsTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("user-preferences",
                Materialized.<String, UserPrefs, KeyValueStore<Bytes, byte[]>>as(
                        Stores.inMemoryKeyValueStore("user-preferences-inmemory").name())
                        .withKeySerde(Serdes.String()).withValueSerde(new UserPrefsSerde()).withCachingDisabled());
    }

    @Bean
    public KTable<StateAndDate, String> doublingRateTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("doubling-rate",
                Materialized.<StateAndDate, String, KeyValueStore<Bytes, byte[]>>as(
                        Stores.inMemoryKeyValueStore("doubling-rate-inmemory").name())
                        .withKeySerde(new StateAndDateSerde()).withValueSerde(stringSerde).withCachingDisabled());
    }

    @Bean
    public KTable<StateAndDate, StatewiseDelta> dailyCountTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("daily-states-count",
                Materialized.<StateAndDate, StatewiseDelta, KeyValueStore<Bytes, byte[]>>as(
                        Stores.inMemoryKeyValueStore("daily-states-count-inmemory").name())
                        .withKeySerde(new StateAndDateSerde()).withValueSerde(new StatewiseDeltaSerde()).withCachingDisabled());
    }

    @Bean
    public KTable<StateAndDate, StatewiseTestData> stateTestTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("statewise-test-data",
                Materialized.<StateAndDate, StatewiseTestData, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("statewise-test-data-persistent").name())
                        .withKeySerde(new StateAndDateSerde()).withValueSerde(new StatewiseTestDataSerde()).withCachingDisabled());
    }

    @Bean
    public KStream<String, PatientAndMessage> individualPatientAlerts(StreamsBuilder kStreamBuilder) {
        final KStream<String, PatientAndMessage> alerts = kStreamBuilder.stream("send-alerts",
                Consumed.with(stringSerde, patientAndMessageSerde));

        // send telegram messages to all subscribers from alerts topic
        alerts
                .peek((patientNumber, patientAndMessage) ->
                        LOG.info("Found new alert for patient #{}. Details: {}", patientNumber, patientAndMessage))
                .mapValues((patientNumber, patientAndMessage) -> {
                    // this check is needed to handle old invalid alerts with different json structure
                    if (isNull(patientAndMessage.getPatientInfo())) {
                        return null;
                    }
                    String alertText = buildAlertText(false, patientAndMessage);
                    final KeyValueIterator<String, UserPrefs> userPrefsIterator = storesManager.userPrefs();
                    userPrefsIterator.forEachRemaining(keyValue -> {
                        UserPrefs prefs = keyValue.value;
                        if (prefs.getMyStates().isEmpty()) {
                            // don't send updates to users who haven't set a preferred state.
                            LOG.info("Skipping update of patient #{} to {} no preferred state found", patientNumber, prefs.getUserId());
                            return;
                        }
                        String patientState = patientAndMessage.getPatientInfo().getDetectedState();
                        if (prefs.getMyStates().contains(patientState)) {
                            // patient update matches user's preferred state
                            LOG.info("Sending telegram alert to {} of patient #{}", prefs.getUserId(), patientNumber);
                            sendTelegramAlert(covid19Bot, prefs.getUserId(), alertText, null, true);
                            return;
                        }
                        LOG.info("Update of patient #{} not relevant to {}", patientNumber, prefs.getUserId());
                    });
                    return PatientAndMessage.builder().message(null).patientInfo(patientAndMessage.getPatientInfo()).build();
                })
                .filter((patientNumber, patientAndMessage) -> nonNull(patientAndMessage))
                .to("posted-messages", Produced.with(stringSerde, patientAndMessageSerde));

        return alerts;
    }
}
