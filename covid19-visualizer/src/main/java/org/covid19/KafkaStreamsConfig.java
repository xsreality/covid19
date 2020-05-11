package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.covid19.district.DistrictwiseData;
import org.covid19.district.DistrictwiseDataSerde;
import org.covid19.district.StateAndDistrict;
import org.covid19.district.StateAndDistrictSerde;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {
    private final KafkaProperties kafkaProperties;
    private Serde<String> stringSerde;
    private StateStoresManager storesManager;

    public KafkaStreamsConfig(KafkaProperties kafkaProperties, StateStoresManager storesManager) {
        this.kafkaProperties = kafkaProperties;
        this.storesManager = storesManager;
        stringSerde = Serdes.String();
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
    public KTable<StateAndDistrict, DistrictwiseData> districtDailyTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("districtwise-daily",
                Materialized.<StateAndDistrict, DistrictwiseData, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("districtwise-daily-persistent").name())
                        .withKeySerde(new StateAndDistrictSerde()).withValueSerde(new DistrictwiseDataSerde()).withCachingDisabled());
    }

    @Bean
    public KTable<StateAndDistrict, DistrictwiseData> districtDeltaTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("districtwise-delta",
                Materialized.<StateAndDistrict, DistrictwiseData, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("districtwise-delta-persistent").name())
                        .withKeySerde(new StateAndDistrictSerde()).withValueSerde(new DistrictwiseDataSerde()).withCachingDisabled());
    }

    @Bean
    public KTable<StateAndDistrict, String> districtZonesTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("zones",
                Materialized.<StateAndDistrict, String, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("zones-persistent").name())
                        .withKeySerde(new StateAndDistrictSerde()).withValueSerde(stringSerde).withLoggingDisabled());
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
                        Stores.persistentKeyValueStore("daily-states-count-persistent").name())
                        .withKeySerde(new StateAndDateSerde()).withValueSerde(new StatewiseDeltaSerde()));
    }

    @Bean
    public KTable<StateAndDate, StatewiseTestData> stateTestTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("statewise-test-data",
                Materialized.<StateAndDate, StatewiseTestData, KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("statewise-test-data-persistent").name())
                        .withKeySerde(new StateAndDateSerde()).withValueSerde(new StatewiseTestDataSerde()));
    }

    @Bean
    public KTable<String, byte[]> visualizationsTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("visualizations",
                Materialized.<String, byte[], KeyValueStore<Bytes, byte[]>>as(
                        Stores.persistentKeyValueStore("visualizations-persistent").name())
                        .withKeySerde(stringSerde).withValueSerde(Serdes.ByteArray()).withCachingDisabled());
    }
}
