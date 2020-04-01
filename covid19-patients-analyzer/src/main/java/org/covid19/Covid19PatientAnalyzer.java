package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

import static java.util.Objects.isNull;

public class Covid19PatientAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19PatientAnalyzer.class);

    private static final String STATE_STORE_CHANGELOG_POSTED_MESSAGES = "state-store-messages";
    private static String APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String CLIENT_ID;
    private static String STREAM_POSTED_MESSAGES;
    private static String STREAM_PATIENTS_DATA;
    private static String STREAM_ALERTS;

    public static void main(final String[] args) {
        initEnv();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<PatientInfo> patientInfoSerde = new PatientInfoSerde();
        final Serde<PatientAndMessage> patientAndMessageSerde = new PatientAndMessageSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PatientInfo> patients = builder.stream(STREAM_PATIENTS_DATA,
                Consumed.with(stringSerde, patientInfoSerde));

        final KTable<String, PatientAndMessage> postedMessages =
                builder.table(STREAM_POSTED_MESSAGES,
                        Consumed.with(stringSerde, patientAndMessageSerde),
                        Materialized.as(STATE_STORE_CHANGELOG_POSTED_MESSAGES));

        patients
                .peek((patientNumber, patientInfo) ->
                        LOG.info("Processing patient number {} with patientInfo {}", patientNumber, patientInfo))
                // clean out upcoming patients with no useful information yet
                .filter(Covid19PatientAnalyzer::cleanData)
                .peek((patientNumber, patientInfo) ->
                        LOG.info("Sufficient info found for patient number {}", patientNumber))
                .leftJoin(postedMessages, (latestPatientInfo, patientAndMessage) -> {
                    // this is a new patient, not alerted before
                    if (Objects.isNull(patientAndMessage)) {
                        LOG.info("Found new patient number (not alerted before {}", latestPatientInfo.getPatientNumber());
                        return new PatientAndMessage(null, latestPatientInfo);
                    }
                    // determine if there is any new information since last update
                    LOG.info("Re-processing patient number {}", latestPatientInfo.getPatientNumber());
                    if (isWorthyUpdate(latestPatientInfo, patientAndMessage.getPatientInfo())) {
                        LOG.info("Found useful updates for patient number {}", latestPatientInfo.getPatientNumber());
                        return new PatientAndMessage(patientAndMessage.getMessage(), latestPatientInfo);
                    }
                    return null;
                })
                .peek((patientNumber, patientAndMessage) ->
                        LOG.info("Patient number {} processed with PatientAndMessage {}", patientNumber, patientAndMessage))
                // filter out unchanged/irrelevant patient information
                .filter((patientNumber, patientAndMessage) -> Objects.nonNull(patientAndMessage))
                .peek((patientNumber, patientAndMessage) ->
                        LOG.info("Patient number {} sending to kafka send-alerts topic", patientNumber))
                .to(STREAM_ALERTS, Produced.with(stringSerde, new PatientAndMessageSerde()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // determine if the latest patient info has significant changes worth sending alerts again.
    private static boolean isWorthyUpdate(PatientInfo latestPatientInfo, PatientInfo oldPatientInfo) {
        if (currentStatusChanged(latestPatientInfo, oldPatientInfo)
                || moreInfoAvailable(latestPatientInfo, oldPatientInfo)) {
            return true;
        }
        // TODO: Add more conditions here to determine worthy update
        LOG.info("No useful updates seen for patient number {}", latestPatientInfo.getPatientNumber());
        return false;
    }

    private static boolean moreInfoAvailable(PatientInfo latestPatientInfo, PatientInfo oldPatientInfo) {
        // null check required because notes field was missed earlier
        if (Objects.nonNull(latestPatientInfo.getNotes()) &&
                !latestPatientInfo.getNotes().equalsIgnoreCase(oldPatientInfo.getNotes())) {
            LOG.info("Detected change in field notes for patient #{}", latestPatientInfo.getPatientNumber());
            return true;
        }
        if (Objects.nonNull(latestPatientInfo.getBackupNotes()) &&
                !latestPatientInfo.getBackupNotes().equalsIgnoreCase(oldPatientInfo.getBackupNotes())) {
            LOG.info("Detected change in field backupnotes for patient #{}", latestPatientInfo.getPatientNumber());
            return true;
        }
        return false;
    }

    private static boolean currentStatusChanged(PatientInfo latestPatientInfo, PatientInfo oldPatientInfo) {
        return !latestPatientInfo.getCurrentStatus().equalsIgnoreCase(oldPatientInfo.getCurrentStatus());
    }

    private static boolean cleanData(String patientNumber, PatientInfo patientInfo) {
        // filter out entries that don't have a current status set.
        // More rules might be added later.
        return currentStatusExists(patientInfo);
    }

    private static boolean currentStatusExists(PatientInfo patientInfo) {
        return Objects.nonNull(patientInfo.getCurrentStatus()) && !patientInfo.getCurrentStatus().isEmpty();
    }

    private static void initEnv() {
        if (isNull(System.getenv("BOOTSTRAP_SERVERS"))) {
            LOG.error("Environment variable BOOTSTRAP_SERVERS must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_APPLICATION_ID"))) {
            LOG.error("Environment variable KAFKA_APPLICATION_ID must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_TOPIC_POSTED_MESSAGES"))) {
            LOG.error("Environment variable KAFKA_TOPIC_POSTED_MESSAGES must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_TOPIC_ALERTS"))) {
            LOG.error("Environment variable KAFKA_TOPIC_ALERTS must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        APPLICATION_ID = System.getenv("KAFKA_APPLICATION_ID");
        CLIENT_ID = APPLICATION_ID + "-client";
        STREAM_PATIENTS_DATA = System.getenv("KAFKA_TOPIC_PATIENTS_DATA");
        STREAM_POSTED_MESSAGES = System.getenv("KAFKA_TOPIC_POSTED_MESSAGES");
        STREAM_ALERTS = System.getenv("KAFKA_TOPIC_ALERTS");
    }
}
