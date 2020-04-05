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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.abilitybots.api.db.MapDBContext;
import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Strings.isNullOrEmpty;
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

    public static void main(final String[] args) {
        initEnv();

        ApiContextInitializer.init();

        Covid19Bot covid19Bot = buildTelegramBot();

        final Properties streamsConfiguration = configureProperties();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<PatientAndMessage> patientAndMessageSerde = new PatientAndMessageSerde();
        final Serde<StatewiseDelta> statewiseDeltaSerde = new StatewiseDeltaSerde();
        final Serde<ArrayList<StatewiseDelta>> statewiseDeltaArrayListSerde = new StatewiseDeltaListSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PatientAndMessage> alerts = builder.stream(STREAM_ALERTS,
                Consumed.with(stringSerde, patientAndMessageSerde));

//        final KStream<String, StatewiseDelta> statewiseDeltaStream =
//                builder.stream("org.covid19.patient-status-stats-statewise-delta-changelog",
//                        Consumed.with(stringSerde, statewiseDeltaSerde));

        final KTable<String, StatewiseDelta> statewiseDeltaTable = builder
                .table("org.covid19.patient-status-stats-statewise-delta-changelog",
                        Materialized.with(stringSerde, statewiseDeltaSerde));

        // send telegram messages to all subscribers from alerts topic
        alerts
                .peek((patientNumber, patientAndMessage) ->
                        LOG.info("Found new alert for patient #{}. Details: {}", patientNumber, patientAndMessage))

                .mapValues((patientNumber, patientAndMessage) -> {
                    // this check is needed to handle old invalid alerts with different json structure
                    if (isNull(patientAndMessage.getPatientInfo())) {
                        return null;
                    }

                    final Map<String, Map<String, String>> userPatientChatHistory = covid19Bot.userPatientChatHistory();

                    final Map<String, Map<String, String>> updatedUserPatientHistory = new HashMap<>();

                    userPatientChatHistory.forEach((userId, patientChatHistory) -> {
                        boolean update = patientChatHistory.containsKey(patientNumber);

                        String alertText = buildAlertText(update, patientAndMessage);

                        Integer messageReplyId = update ? Integer.valueOf(patientChatHistory.get(patientNumber)) : null;

                        LOG.info("Sending telegram alert to {} as {} message of patient #{}",
                                userId, update ? "update" : "original", patientNumber);
                        sendTelegramAlert(covid19Bot, userId, alertText, messageReplyId)
                                .map(telegramResponse ->
                                        patientChatHistory.put(patientNumber, String.valueOf(telegramResponse.getMessageId())));

                        updatedUserPatientHistory.put(userId, patientChatHistory);
                    });

                    covid19Bot.updateChatHistory(updatedUserPatientHistory);

                    // this is now redundant as we store the patientNumber->messageId mapping in Telegram Embedded DB
                    return PatientAndMessage.builder()
                            .message(null)
                            .patientInfo(patientAndMessage.getPatientInfo())
                            .build();
                })
                .filter((patientNumber, patientAndMessage) -> nonNull(patientAndMessage))
                .to(STREAM_POSTED_MESSAGES, Produced.with(stringSerde, patientAndMessageSerde));

        // build topology for processing statewise stats and sending Telegram Alerts
        statewiseDeltaTable
                .groupBy((key, value) -> KeyValue.pair("1", value), Grouped.with(stringSerde, statewiseDeltaSerde))
                .aggregate(ArrayList::new,
                        (key, newDelta, aggregate) -> { // build arraylist in adder
                            aggregate.add(newDelta);
                            return aggregate;
                        },
                        (key, oldDelta, aggregate) -> aggregate, // no-op in subtractor
                        Materialized.<String, ArrayList<StatewiseDelta>, KeyValueStore<Bytes, byte[]>>as("collection")
                                .withKeySerde(stringSerde)
                                .withValueSerde(statewiseDeltaArrayListSerde))
                .toStream()
                .foreach((key, deltas) -> fireStatewiseTelegramAlert(covid19Bot, deltas));

        // send telegram messages for statewise delta changes of recovered, deaths and confirmed cases
//        statewiseDeltaStream
//                .peek((key, value) -> LOG.info("processing stream key {}, value {}", key, value))
//                .selectKey((key, value) -> "1")
//                .groupByKey(Grouped.with(stringSerde, statewiseDeltaSerde))
//                .aggregate(ArrayList::new, (key, newDelta, aggregate) -> {
//                    // search the list of deltas to check if new delta
//                    // is for an existing state. If found, sum the delta values
//                    AtomicBoolean found = new AtomicBoolean(false);
//                    aggregate.forEach(delta -> {
//                        if (delta.getState().equalsIgnoreCase(newDelta.getState())) {
//                            found.set(true);
//                            delta.setDeltaRecovered(delta.getDeltaRecovered() + newDelta.getDeltaRecovered());
//                            delta.setDeltaDeaths(delta.getDeltaDeaths() + newDelta.getDeltaDeaths());
//                            delta.setDeltaConfirmed(delta.getDeltaConfirmed() + newDelta.getDeltaConfirmed());
//                        }
//                    });
//                    // if not found, it is a new Indian State in the window so add it to the list.
//                    if (!found.get()) {
//                        aggregate.add(newDelta);
//                    }
//                    return aggregate;
//                }, Materialized.<String, ArrayList<StatewiseDelta>, KeyValueStore<Bytes, byte[]>>as("30-secs-window")
//                        .withKeySerde(stringSerde)
//                        .withValueSerde(statewiseDeltaArrayListSerde))
//                .toStream()
//                .peek((key, deltas) -> {
//                    deltas.forEach(delta -> {
//                        LOG.info("State: {}, deltaRecovered: {}, deltaDeaths: {}, deltaConfirmed: {}",
//                                delta.getState(), delta.getDeltaRecovered(), delta.getDeltaDeaths(), delta.getDeltaConfirmed());
//                    });
//                })
//                .foreach((key, deltas) -> {
//                    // build alert text and send telegram update
//                    AtomicReference<String> updateText = new AtomicReference<>("");
//                    deltas.forEach(delta -> {
//                        // skip total
//                        if ("total".equalsIgnoreCase(delta.getState())) {
//                            return;
//                        }
//
//                        boolean include = false;
//                        String textLine = "";
//                        if (delta.getDeltaConfirmed() > 0L) {
//                            include = true;
//                            textLine = textLine.concat(String.format("%d new cases ", delta.getDeltaConfirmed()));
//                        }
//                        if (delta.getDeltaDeaths() > 0L) {
//                            include = true;
//                            textLine = textLine.concat(String.format("%d death(s) ", delta.getDeltaDeaths()));
//                        }
//                        if (delta.getDeltaRecovered() > 0L) {
//                            include = true;
//                            textLine = textLine.concat(String.format("%d recovery ", delta.getDeltaRecovered()));
//                        }
//                        if (include) {
//                            textLine = textLine.concat(String.format("in %s\n", delta.getState()));
//                        }
////                        LOG.info("textLine is: {}", textLine);
//                        updateText.accumulateAndGet(textLine, (current, update) -> current + update);
////                        LOG.info("updateText is: {}", updateText);
//                    });
//
////                    sendTelegramAlert(covid19Bot, TELEGRAM_CREATOR_ID, updateText.get(), null);
//                    LOG.info("Sending alert with text: {}", updateText.get());
//                });


        final KafkaStreams streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);

        // open keyvalue store for reading total statewise stats
//        final ReadOnlyKeyValueStore<String, StatewiseStats> statewiseStore = streams.store("statewise-data", QueryableStoreTypes.keyValueStore());
//        if (isNull(statewiseStore)) {
//            throw new IllegalStateException("Unable to open store statewise data");
//        }

        LOG.info("{}", builder.build().describe());

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void fireStatewiseTelegramAlert(Covid19Bot covid19Bot, ArrayList<StatewiseDelta> deltas) {
        // build alert text and send telegram update
        AtomicReference<String> updateText = new AtomicReference<>("");
        deltas.forEach(delta -> buildStatewiseAlertText(updateText, delta));
        String lastUpdated = deltas.get(0).getLastUpdatedTime();
        sendTelegramAlert(covid19Bot, TELEGRAM_CREATOR_ID, lastUpdated + "\n\n" + updateText.get(), null);
    }

    private static void buildStatewiseAlertText(AtomicReference<String> updateText, StatewiseDelta delta) {
        // skip total
        if ("total".equalsIgnoreCase(delta.getState())) {
            return;
        }

        boolean include = false;
        String textLine = "";
        if (delta.getDeltaConfirmed() > 0L) {
            include = true;
            textLine = textLine.concat(String.format("%d new case(s) ", delta.getDeltaConfirmed()));
        }
        if (delta.getDeltaDeaths() > 0L) {
            include = true;
            textLine = textLine.concat(String.format("%d death(s) ", delta.getDeltaDeaths()));
        }
        if (delta.getDeltaRecovered() > 0L) {
            include = true;
            textLine = textLine.concat(String.format("%d recovered ", delta.getDeltaRecovered()));
        }
        if (include) {
            textLine = textLine.concat(String.format("in %s\n", delta.getState()));
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

    private static Optional<Message> sendTelegramAlert(Covid19Bot bot, String chatId, String alertText, Integer replyId) {
        try {
            Thread.sleep(50);  // to avoid hitting Telegram rate limits
            SendMessage telegramMessage = new SendMessage()
                    .setChatId(chatId)
                    .setText(alertText)
                    .disableNotification()
                    .setReplyToMessageId(replyId);

            return Optional.ofNullable(bot.execute(telegramMessage));
        } catch (TelegramApiException | InterruptedException e) {
            LOG.error("Unable to send Telegram alert!", e);
            return Optional.empty();
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
}