package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.abilitybots.api.db.MapDBContext;
import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Covid19TelegramApp {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19TelegramApp.class);

    private static String APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String CLIENT_ID;
    private static String STREAM_POSTED_MESSAGES;
    private static String STREAM_ALERTS;
    private static String TELEGRAM_BOT_TOKEN;
    private static String TELEGRAM_BOT_USERNAME;
    private static String TELEGRAM_DB_PATH;
    private static String CHANNEL_CHAT_ID;
    private static String TELEGRAM_CREATOR_ID;

    public static void main(final String[] args) throws InterruptedException {
        initEnv();

        ApiContextInitializer.init();

        Covid19Bot covid19Bot = buildTelegramBot();

        final Properties streamsConfiguration = configureProperties();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<PatientAndMessage> patientAndMessageSerde = new PatientAndMessageSerde();
        final Serde<StatewiseStats> statewiseStatsSerde = new StatewiseStatsSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PatientAndMessage> alerts = builder.stream(STREAM_ALERTS,
                Consumed.with(stringSerde, patientAndMessageSerde));

        final KTable<String, StatewiseStats> statewiseStatsTable = builder
                .table("statewise-data",
                        Materialized.<String, StatewiseStats, KeyValueStore<Bytes, byte[]>>as(
                                Stores.persistentKeyValueStore("statewise-stats-persistent").name())
                                .withKeySerde(stringSerde).withValueSerde(statewiseStatsSerde).withCachingDisabled());

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
                .to(STREAM_POSTED_MESSAGES, Produced.with(stringSerde, patientAndMessageSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);

        LOG.info("{}", builder.build().describe()); // print topology

        streams.start();

        // open keyvalue store for reading total statewise stats
        final ReadOnlyKeyValueStore<String, StatewiseStats> statewiseStore =
                waitUntilStoreIsQueryable(statewiseStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore(), streams);

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        /* Setup the Consumer for statewise alerting */

        Properties consumerProps = new Properties();
        consumerProps.put("client.id", APPLICATION_ID + "-consumer-client");
        consumerProps.put("group.id", APPLICATION_ID + "-consumer");
        consumerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, stringSerde.deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        consumerProps.put("json.value.type", StatewiseDelta.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, StatewiseDelta> statewiseDeltaConsumer = new KafkaConsumer<>(consumerProps);
        statewiseDeltaConsumer.subscribe(singletonList("org.covid19.patient-status-stats-statewise-delta-changelog"));

        try {
            List<StatewiseDelta> readyToSend = new ArrayList<>();
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, StatewiseDelta> records = statewiseDeltaConsumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<String, StatewiseDelta> record : records) {
                    StatewiseDelta delta = record.value();
                    // skip update with no changes
                    if (delta.getDeltaRecovered() < 1L && delta.getDeltaConfirmed() < 1L && delta.getDeltaDeaths() < 1L) {
                        continue;
                    }
                    readyToSend.add(delta);
                }
                if (readyToSend.isEmpty()) {
                    continue;
                }
                List<StatewiseStats> stats = new ArrayList<>();
                readyToSend.forEach(statewiseDelta -> {
                    stats.add(statewiseStore.get(statewiseDelta.getState()));
                });
                fireStatewiseTelegramAlert(covid19Bot, buildStatewiseAlertText(stats, readyToSend));
                readyToSend.clear();
            }
        } finally {
            statewiseDeltaConsumer.close();
        }
    }

    static String buildStatewiseAlertText(List<StatewiseStats> total, List<StatewiseDelta> deltas) {
        String lastUpdated = deltas.get(deltas.size() - 1).getLastUpdatedTime();
        AtomicReference<String> alertText = new AtomicReference<>("");
        deltas.forEach(delta -> buildDeltaAlertLine(alertText, delta));
        if (alertText.get().isEmpty() || "\n".equalsIgnoreCase(alertText.get())) {
            LOG.info("No useful update to alert on. Skipping...");
            return "";
        }
        buildSummaryAlertBlock(alertText, total);
        String finalText = String.format("<i>%s</i>\n\n%s", lastUpdated, alertText.get());
        LOG.info("Statewise Alert text generated:\n{}", finalText);
        return finalText;
    }

    private static void fireStatewiseTelegramAlert(Covid19Bot covid19Bot, String alertText) {
        if (isNull(alertText) || alertText.isEmpty()) {
            return; // skip sending alert
        }
        final List<String> subscribedUsers = covid19Bot.subscribedUsers();
        subscribedUsers.forEach(subscriber -> {
            LOG.info("Sending statewise updates to {}", subscriber);
            sendTelegramAlert(covid19Bot, subscriber, alertText, null, true);
        });
    }

    static void buildSummaryAlertBlock(AtomicReference<String> updateText, List<StatewiseStats> stats) {
        stats.forEach(stat -> {
            String statText = String.format("\n<i>%s</i>\n<pre>\nTotal cases: %s\nRecovered: %s\nDeaths: %s\n</pre>\n",
                    stat.getState(), stat.getConfirmed(), stat.getRecovered(), stat.getDeaths());
            updateText.accumulateAndGet(statText, (current, update) -> current + update);
        });
    }

    static void buildDeltaAlertLine(AtomicReference<String> updateText, StatewiseDelta delta) {
        // skip total
        if ("total".equalsIgnoreCase(delta.getState())) {
            return;
        }

        boolean confirmed = false, deaths = false, recovered = false, include = false;
        String textLine = "";
        if (delta.getDeltaConfirmed() > 0L) {
            include = true;
            confirmed = true;
            textLine = textLine.concat(String.format("%d new %s",
                    delta.getDeltaConfirmed(),
                    delta.getDeltaConfirmed() == 1L ? "case" : "cases"));
        }
        if (delta.getDeltaDeaths() > 0L) {
            deaths = true;
            include = true;
            textLine = textLine.concat(String.format("%s%d %s",
                    confirmed ? ", " : "",
                    delta.getDeltaDeaths(),
                    delta.getDeltaDeaths() == 1L ? "death" : "deaths"));
        }
        if (delta.getDeltaRecovered() > 0L) {
            recovered = true;
            include = true;
            textLine = textLine.concat(String.format("%s%d %s",
                    confirmed || deaths ? ", " : "",
                    delta.getDeltaRecovered(),
                    delta.getDeltaRecovered() == 1L ? "recovery" : "recoveries"));
        }
        if (include) {
            textLine = textLine.concat(String.format( " in %s\n",
                    delta.getState()));
        }
        updateText.accumulateAndGet(textLine, (current, update) -> current + update);
    }

    @NotNull
    private static Properties configureProperties() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        return streamsConfiguration;
    }

    private static Covid19Bot buildTelegramBot() {
        DB db = DBMaker
                .fileDB(new File(TELEGRAM_DB_PATH))
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .transactionEnable()
                .make();

        TelegramBotsApi botsApi = new TelegramBotsApi();
        Covid19Bot covid19Bot = new Covid19Bot(TELEGRAM_BOT_TOKEN, TELEGRAM_BOT_USERNAME, new MapDBContext(db), TELEGRAM_CREATOR_ID, CHANNEL_CHAT_ID);

        try {
            botsApi.registerBot(covid19Bot);
        } catch (TelegramApiException e) {
            throw new IllegalStateException("Unable to register Telegram bot", e);
        }
        return covid19Bot;
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
        if (isNull(System.getenv("TELEGRAM_BOT_TOKEN"))) {
            LOG.error("Environment variable TELEGRAM_BOT_TOKEN must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("TELEGRAM_BOT_USERNAME"))) {
            LOG.error("Environment variable TELEGRAM_BOT_TOKEN must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("TELEGRAM_CHAT_ID"))) {
            LOG.error("Environment variable TELEGRAM_CHAT_ID must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("TELEGRAM_CREATOR_ID"))) {
            LOG.error("Environment variable TELEGRAM_CREATOR_ID must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("TELEGRAM_DB_PATH"))) {
            LOG.error("Environment variable TELEGRAM_DB_PATH must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        APPLICATION_ID = System.getenv("KAFKA_APPLICATION_ID");
        CLIENT_ID = APPLICATION_ID + "-client";
        STREAM_POSTED_MESSAGES = System.getenv("KAFKA_TOPIC_POSTED_MESSAGES");
        STREAM_ALERTS = System.getenv("KAFKA_TOPIC_ALERTS");
        TELEGRAM_BOT_TOKEN = System.getenv("TELEGRAM_BOT_TOKEN");
        TELEGRAM_BOT_USERNAME = System.getenv("TELEGRAM_BOT_USERNAME");
        TELEGRAM_DB_PATH = System.getenv("TELEGRAM_DB_PATH");
        CHANNEL_CHAT_ID = System.getenv("TELEGRAM_CHAT_ID");
        TELEGRAM_CREATOR_ID = System.getenv("TELEGRAM_CREATOR_ID");
    }

    private static void sendTelegramAlert(Covid19Bot bot, String chatId, String alertText, Integer replyId, boolean notification) {
        try {
            Thread.sleep(50);  // to avoid hitting Telegram rate limits
            SendMessage telegramMessage = new SendMessage()
                    .setChatId(chatId)
                    .setText(alertText)
                    .enableHtml(true)
                    .setReplyToMessageId(replyId);

            telegramMessage = notification ? telegramMessage.enableNotification() : telegramMessage.disableNotification();

            bot.execute(telegramMessage);
        } catch (TelegramApiException | InterruptedException e) {
            LOG.error("Unable to send Telegram alert to user {}, with error {}", chatId, e.getMessage());
        }
    }

    private static String buildAlertText(boolean update, PatientAndMessage patientAndMessage) {
        PatientInfo patientInfo = patientAndMessage.getPatientInfo();
        String alertText;
        if (update) {
            alertText = String.format("Update on patient #%s\n\n", patientInfo.getPatientNumber());
        } else {
            alertText = String.format("Patient #%s\n\n", patientInfo.getPatientNumber());
        }

        alertText = alertText.concat(String.format("Status: %s\n\n", patientInfo.getCurrentStatus()));

        if (!isNullOrEmpty(patientInfo.getAgeBracket())) {
            alertText = alertText.concat(String.format("Age: %s\n", patientInfo.getAgeBracket()));
        }

        if (!isNullOrEmpty(patientInfo.getGender())) {
            String gender = "M".equalsIgnoreCase(patientInfo.getGender()) ? "Male" : "Female";
            alertText = alertText.concat(String.format("Gender: %s\n", gender));
        }

        alertText = alertText.concat(String.format("Place: %s %s\n", patientInfo.getDetectedCity(), patientInfo.getDetectedState()));

        if (!isNullOrEmpty(patientInfo.getD180G())) {
            alertText = alertText.concat(String.format("Possible cause: %s\n", patientInfo.getD180G()));
        } else if (!isNullOrEmpty(patientInfo.getNotes())) {
            alertText = alertText.concat(String.format("Possible cause: %s\n", patientInfo.getNotes()));
        } else if (!isNullOrEmpty(patientInfo.getBackupNotes())) {
            alertText = alertText.concat(String.format("Possible cause: %s\n", patientInfo.getBackupNotes()));
        }

        if (!isNullOrEmpty(patientInfo.getDateAnnounced())) {
            alertText = alertText.concat(String.format("Announced: %s\n", patientInfo.getDateAnnounced()));
        }

        if (!isNullOrEmpty(patientInfo.getSource1())
                || !isNullOrEmpty(patientInfo.getSource2())
                || !isNullOrEmpty(patientInfo.getSource3())) {
            alertText = alertText.concat("\nSources:\n");
        }

        if (!isNullOrEmpty(patientInfo.getSource1())) {
            alertText = alertText.concat(String.format("%s\n", patientInfo.getSource1()));
        }
        if (!isNullOrEmpty(patientInfo.getSource2())) {
            alertText = alertText.concat(String.format("%s\n", patientInfo.getSource2()));
        }
        if (!isNullOrEmpty(patientInfo.getSource3())) {
            alertText = alertText.concat(String.format("%s\n", patientInfo.getSource3()));
        }

        LOG.info("Alert Text built for patient #{}:\n{}", patientInfo.getPatientNumber(), alertText);

        return alertText;
    }

    public static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                  final QueryableStoreType<T> queryableStoreType,
                                                  final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }
}