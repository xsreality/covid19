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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Properties;

import static java.time.Duration.ofDays;
import static java.time.ZoneId.of;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Covid19PatientAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19PatientAnalyzer.class);

    private static final String STATE_STORE_CHANGELOG_POSTED_MESSAGES = "state-store-messages";
    private static String APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String CLIENT_ID;
    private static String STREAM_POSTED_MESSAGES;
    private static String STREAM_PATIENTS_DATA;
    private static String STREAM_ALERTS;
    private static String STATE_DIR;

    private static final String NEWS_STORE = "news-store";

    public static void main(final String[] args) {
        initEnv();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
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

        final KStream<String, PatientInfo> cleanedPatients = patients
                // clean out upcoming patients with no useful information yet
                .filter(Covid19PatientAnalyzer::cleanData);

        cleanedPatients
                .leftJoin(postedMessages, (latestPatientInfo, patientAndMessage) -> {
                    if (isHospitalized(latestPatientInfo)) {
                        // we skip hospitalized patients as they are too many now.
                        return null;
                    }
                    // this is a new patient, not alerted before
                    if (Objects.isNull(patientAndMessage)) {
                        LOG.info("Found new patient number (not alerted before {})", latestPatientInfo.getPatientNumber());
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
                // filter out unchanged/irrelevant patient information
                .filter((patientNumber, patientAndMessage) -> nonNull(patientAndMessage))
                .peek((patientNumber, patientAndMessage) ->
                        LOG.info("Patient number {} sending to kafka send-alerts topic", patientNumber))
                .to(STREAM_ALERTS, Produced.with(stringSerde, new PatientAndMessageSerde()));

        // How long we "remember" an event.  During this time, any incoming duplicates of the event
        // will be, well, dropped, thereby de-duplicating the input data.
        //
        // The actual value depends on your use case.  To reduce memory and disk usage, you could
        // decrease the size to purge old windows more frequently at the cost of potentially missing out
        // on de-duplicating late-arriving records.
        final Duration windowSize = ofDays(7L);

        // retention period must be at least window size -- for this use case, we don't need a longer retention period
        // and thus just use the window size as retention time
        final Duration retentionPeriod = windowSize;

        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(NEWS_STORE, retentionPeriod, windowSize, false),
                Serdes.String(),
                Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);

        final KStream<String, PatientInfo> deduplicated = cleanedPatients.transform(
                () -> new DeduplicationTransformer<>(windowSize.toMillis(), (key, value) -> bestNewsSource(value), NEWS_STORE),
                NEWS_STORE);
        deduplicated
                .filter((key, patientInfo) -> isWithinPastWeek(patientInfo.getDateAnnounced()))
                .selectKey((key, value) -> value.getDetectedState())
                .mapValues((readOnlyKey, patientInfo) -> bestNewsSource(patientInfo))
                .filter((key, news) -> nonNull(news))
                .to("news-sources", Produced.with(stringSerde, stringSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        LOG.info("{}", builder.build().describe());

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static boolean isWithinPastWeek(String date) {
        if (isNull(date) || date.isEmpty()) {
            return false; // can't determine, so skip it.
        }
        LocalDate announcedDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        LocalDate today = LocalDate.now(of("Asia/Kolkata"));
        LocalDate lastWeek = today.minusWeeks(1L);

        return announcedDate.compareTo(today) <= 0 && announcedDate.compareTo(lastWeek) > 0;
    }

    private static String bestNewsSource(PatientInfo patientInfo) {
        if (!patientInfo.getSource3().isEmpty()) {
            return patientInfo.getSource3();
        }
        if (!patientInfo.getSource2().isEmpty()) {
            return patientInfo.getSource2();
        }
        if (!patientInfo.getSource1().isEmpty()) {
            return patientInfo.getSource1();
        }
        return null;
    }

    private static boolean isHospitalized(PatientInfo latestPatientInfo) {
        return "Hospitalized".equalsIgnoreCase(latestPatientInfo.getCurrentStatus());
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
        if (nonNull(latestPatientInfo.getNotes()) &&
                !latestPatientInfo.getNotes().equalsIgnoreCase(oldPatientInfo.getNotes())) {
            LOG.info("Detected change in field notes for patient #{}", latestPatientInfo.getPatientNumber());
            return true;
        }
        if (nonNull(latestPatientInfo.getBackupNotes()) &&
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
        return nonNull(patientInfo.getCurrentStatus()) && !patientInfo.getCurrentStatus().isEmpty();
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
        if (isNull(System.getenv("KAFKA_TOPIC_PATIENTS_DATA"))) {
            LOG.error("Environment variable KAFKA_TOPIC_PATIENTS_DATA must be set!");
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
        if (isNull(System.getenv("KAFKA_STATE_DIR"))) {
            LOG.error("Environment variable KAFKA_STATE_DIR must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        APPLICATION_ID = System.getenv("KAFKA_APPLICATION_ID");
        CLIENT_ID = APPLICATION_ID + "-client";
        STREAM_PATIENTS_DATA = System.getenv("KAFKA_TOPIC_PATIENTS_DATA");
        STREAM_POSTED_MESSAGES = System.getenv("KAFKA_TOPIC_POSTED_MESSAGES");
        STREAM_ALERTS = System.getenv("KAFKA_TOPIC_ALERTS");
        STATE_DIR = System.getenv("KAFKA_STATE_DIR");
    }
}
