package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Covid19Stats {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Stats.class);

    private static String KAFKA_GLOBAL_STATS_APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String KAFKA_GLOBAL_STATS_CLIENT_ID;
    private static String KAFKA_TOPIC_PATIENT_STATUS_COUNT;
    private static String KAFKA_TOPIC_PATIENT_ALERTS;

    public static void main(final String[] args) {
        initEnv();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GLOBAL_STATS_APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, KAFKA_GLOBAL_STATS_CLIENT_ID);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 20 * 1024 * 1024L);
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<PatientAndMessage> patientAndMessageSerde = new PatientAndMessageSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        // build KStream from patient alerts topic
        final KStream<String, PatientAndMessage> patientAlerts = builder.stream(KAFKA_TOPIC_PATIENT_ALERTS, Consumed.with(stringSerde, patientAndMessageSerde));

        // build KStream of patient and current status and store in new topic
        final KStream<String, String> patientCurrentStatus = patientAlerts
                // need these filters to clear out old invalid data
                .filter((patientNumber, patientAndMessage) ->
                        nonNull(patientAndMessage.getPatientInfo()) && nonNull(patientNumber) && !patientNumber.isEmpty() && !patientAndMessage.getPatientInfo().getCurrentStatus().isEmpty())
                .mapValues((patientNumber, patientAndMessage) -> patientAndMessage.getPatientInfo().getCurrentStatus());

        patientCurrentStatus.to("patient-current-status", Produced.with(stringSerde, stringSerde));

        // read the topic as KTable so that updates are read correctly
        final KTable<String, String> patientCurrentStatusTable = builder.table("patient-current-status", Materialized.with(stringSerde, stringSerde));

        // count the occurences of current status
        KTable<String, Long> currentStatusCount = patientCurrentStatusTable
                .groupBy((patientNumber, patientStatus) -> KeyValue.pair(patientStatus, patientStatus), Grouped.with(stringSerde, stringSerde))
                .count(Materialized.with(stringSerde, longSerde));

        // output results to a topic
        currentStatusCount.toStream()
                .peek((key, value) -> LOG.info("Status: {}, Count: {}", key, value))
                .to(KAFKA_TOPIC_PATIENT_STATUS_COUNT, Produced.with(stringSerde, longSerde));

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
        if (isNull(System.getenv("KAFKA_TOPIC_PATIENT_ALERTS"))) {
            LOG.error("Environment variable KAFKA_TOPIC_PATIENT_ALERTS must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_TOPIC_PATIENT_STATUS_COUNT"))) {
            LOG.error("Environment variable KAFKA_TOPIC_PATIENT_STATUS_COUNT must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        KAFKA_GLOBAL_STATS_APPLICATION_ID = System.getenv("KAFKA_GLOBAL_STATS_APPLICATION_ID");
        KAFKA_GLOBAL_STATS_CLIENT_ID = KAFKA_GLOBAL_STATS_APPLICATION_ID + "-client";
        KAFKA_TOPIC_PATIENT_ALERTS = System.getenv("KAFKA_TOPIC_PATIENT_ALERTS");
        KAFKA_TOPIC_PATIENT_STATUS_COUNT = System.getenv("KAFKA_TOPIC_PATIENT_STATUS_COUNT");
    }
}
