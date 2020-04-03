package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Covid19Stats {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Stats.class);

    private static final String STATE_STORE_REPARTITION_BY_STATUS = "state-store-patient-status";
    private static final String STATE_STORE_CHANGELOG_STATUS_COUNT = "state-store-patient-status-count";
    private static final String STATE_STORE_CHANGELOG_LATEST_PATIENT_DATA = "state-store-patient-data";

    private static String KAFKA_GLOBAL_STATS_APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String KAFKA_GLOBAL_STATS_CLIENT_ID;
    private static String KAFKA_TOPIC_PATIENT_STATUS_COUNT;
    private static String KAFKA_TOPIC_PATIENT_DATA;

    public static void main(final String[] args) {
        initEnv();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GLOBAL_STATS_APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, KAFKA_GLOBAL_STATS_CLIENT_ID);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 20 * 1024 * 1024L);
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<PatientAndMessage> patientAndMessageSerde = new PatientAndMessageSerde();
        final Serde<PatientStatusCount> patientStatusCountSerde = new PatientStatusCountSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, PatientAndMessage> patientData = builder.table(KAFKA_TOPIC_PATIENT_DATA,
                Consumed.with(stringSerde, patientAndMessageSerde),
                Materialized.<String, PatientAndMessage>as(Stores.inMemoryKeyValueStore("myStore")).withCachingDisabled());
//        final KTable<String, PatientInfo> patientData = builder.table(KAFKA_TOPIC_PATIENT_DATA,
//                Materialized.<String, PatientInfo, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_CHANGELOG_LATEST_PATIENT_DATA).withKeySerde(stringSerde).withValueSerde(patientInfoSerde).withCachingEnabled());
        //Materialized.<String, PatientInfo>as(STATE_STORE_CHANGELOG_LATEST_PATIENT_DATA)
        //      .withKeySerde(stringSerde).withValueSerde(patientInfoSerde));
        patientData
                .filter((patientNumber, patientAndMessage) ->
                        nonNull(patientAndMessage.getPatientInfo()) && nonNull(patientNumber) && !patientNumber.isEmpty() && !patientAndMessage.getPatientInfo().getCurrentStatus().isEmpty())
                .groupBy((patientNumber, patientAndMessage) -> KeyValue.pair(patientAndMessage.getPatientInfo().getCurrentStatus(), patientAndMessage.getPatientInfo().getPatientNumber()),
                        Grouped.<String, String>as(STATE_STORE_REPARTITION_BY_STATUS)
                                .withKeySerde(stringSerde)
                                .withValueSerde(stringSerde))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_CHANGELOG_STATUS_COUNT)
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde))
                .toStream()
                .peek((status, count) -> LOG.info("Status: {}, count: {}", status, count));

//                .toStream()
//                .filter((patientNumber, patientInfo) -> nonNull(patientInfo) && nonNull(patientNumber) && !patientNumber.isEmpty())
//                .map((patientNumber, patientInfo) -> KeyValue.pair(patientInfo.getCurrentStatus(), patientInfo.getPatientNumber()))
//                .groupByKey(Grouped.<String, String>as(STATE_STORE_REPARTITION_BY_STATUS)
//                        .withKeySerde(stringSerde)
//                        .withValueSerde(stringSerde))
//                .aggregate(PatientStatusCount::new, (patientStatus, patientNumber, aggregate) -> {
//                    aggregate.setStatus(patientStatus);
//                    aggregate.setCount(aggregate.getCount() + 1);
//                    return aggregate;
//                }, Materialized.<String, PatientStatusCount, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_CHANGELOG_STATUS_COUNT).withKeySerde(stringSerde).withValueSerde(patientStatusCountSerde));
//                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_CHANGELOG_STATUS_COUNT)
//                        .withKeySerde(stringSerde)
//                        .withValueSerde(longSerde))
//                .toStream()
//                .peek((status, count) -> LOG.info("Status: {}, count: {}", status, count));
//                .to(KAFKA_TOPIC_PATIENT_STATUS_COUNT, Produced.with(stringSerde, longSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void initEnv() {
        if (isNull(System.getenv("BOOTSTRAP_SERVERS"))) {
            LOG.error("Environment variable BOOTSTRAP_SERVERS must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_GLOBAL_STATS_APPLICATION_ID"))) {
            LOG.error("Environment variable KAFKA_GLOBAL_STATS_APPLICATION_ID must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_TOPIC_PATIENT_DATA"))) {
            LOG.error("Environment variable KAFKA_TOPIC_PATIENT_DATA must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_TOPIC_PATIENT_STATUS_COUNT"))) {
            LOG.error("Environment variable KAFKA_TOPIC_PATIENT_STATUS_COUNT must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        KAFKA_GLOBAL_STATS_APPLICATION_ID = System.getenv("KAFKA_GLOBAL_STATS_APPLICATION_ID");
        KAFKA_GLOBAL_STATS_CLIENT_ID = KAFKA_GLOBAL_STATS_APPLICATION_ID + "-client";
        KAFKA_TOPIC_PATIENT_DATA = System.getenv("KAFKA_TOPIC_PATIENT_DATA");
        KAFKA_TOPIC_PATIENT_STATUS_COUNT = System.getenv("KAFKA_TOPIC_PATIENT_STATUS_COUNT");
    }
}
