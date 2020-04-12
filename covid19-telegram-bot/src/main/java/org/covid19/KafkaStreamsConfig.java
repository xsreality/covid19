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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.List;
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

    public KafkaStreamsConfig(KafkaProperties kafkaProperties, Covid19Bot covid19Bot) {
        this.kafkaProperties = kafkaProperties;
        this.covid19Bot = covid19Bot;
        stringSerde = Serdes.String();
        patientAndMessageSerde = new PatientAndMessageSerde();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> kafkaStreamsProps = new HashMap<>(kafkaProperties.buildStreamsProperties());
        kafkaStreamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaStreamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        kafkaStreamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        kafkaStreamsProps.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
//        kafkaStreamsProps.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new KafkaStreamsConfiguration(kafkaStreamsProps);
    }

    @Bean
    public KTable<String, StatewiseDelta> dailyStatsTable(StreamsBuilder streamsBuilder) {
        // used to create keyvalue store
        return streamsBuilder.table("statewise-daily-stats",
                Materialized.<String, StatewiseDelta, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("statewise-daily-persistent").name())
                        .withKeySerde(Serdes.String()).withValueSerde(new StatewiseDeltaSerde()).withCachingDisabled());
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
                    final List<String> subscribedUsers = covid19Bot.subscribedUsers();
                    String alertText = buildAlertText(false, patientAndMessage);
                    subscribedUsers.forEach(userId -> {
                        LOG.info("Sending telegram alert to {} of patient #{}", userId, patientNumber);
                        sendTelegramAlert(covid19Bot, userId, alertText, null, false);
                    });
                    // this is now redundant as we store the patientNumber->messageId mapping in Telegram Embedded DB
                    return PatientAndMessage.builder().message(null).patientInfo(patientAndMessage.getPatientInfo()).build();
                })
                .filter((patientNumber, patientAndMessage) -> nonNull(patientAndMessage))
                .to("posted-messages", Produced.with(stringSerde, patientAndMessageSerde));

        return alerts;
    }
}
