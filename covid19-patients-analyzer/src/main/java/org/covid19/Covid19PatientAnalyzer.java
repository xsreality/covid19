package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static java.time.Duration.ofDays;
import static java.time.ZoneId.of;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Covid19PatientAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19PatientAnalyzer.class);

    private static String APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String CLIENT_ID;
    private static String STREAM_PATIENTS_DATA;
    private static String STATE_DIR;

    private static final String NEWS_STORE = "news-store";

    public static void main(final String[] args) {
        initEnv();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<PatientInfo> patientInfoSerde = new PatientInfoSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PatientInfo> patients = builder.stream(STREAM_PATIENTS_DATA, Consumed.with(stringSerde, patientInfoSerde));

        // clean out upcoming patients with no useful information yet
        final KStream<String, PatientInfo> cleanedPatients = patients.filter(Covid19PatientAnalyzer::cleanData);

        // How long we "remember" an event.  During this time, any incoming duplicates of the event
        // will be, well, dropped, thereby de-duplicating the input data.
        //
        // The actual value depends on your use case.  To reduce memory and disk usage, you could
        // decrease the size to purge old windows more frequently at the cost of potentially missing out
        // on de-duplicating late-arriving records.
        final Duration windowSize = ofDays(7L);

        // retention period must be at least window size -- for this use case, we don't need a longer retention period
        // and thus just use the window size as retention time
        //noinspection UnnecessaryLocalVariable
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
                .peek((state, value) -> LOG.info("Found updated news source for state {}: {}", state, value))
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
        if (isNull(System.getenv("KAFKA_STATE_DIR"))) {
            LOG.error("Environment variable KAFKA_STATE_DIR must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        APPLICATION_ID = System.getenv("KAFKA_APPLICATION_ID");
        CLIENT_ID = APPLICATION_ID + "-client";
        STREAM_PATIENTS_DATA = System.getenv("KAFKA_TOPIC_PATIENTS_DATA");
        STATE_DIR = System.getenv("KAFKA_STATE_DIR");
    }
}
