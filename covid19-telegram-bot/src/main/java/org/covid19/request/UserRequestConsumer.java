package org.covid19.request;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.covid19.StateStoresManager;
import org.covid19.StatewiseDelta;
import org.covid19.StatewiseTestData;
import org.covid19.UserRequest;
import org.covid19.bot.Covid19Bot;
import org.covid19.district.DistrictwiseData;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.handler.annotation.Payload;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

import static io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;
import static java.lang.Integer.parseInt;
import static java.time.ZoneId.of;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.covid19.bot.BotUtils.buildStateSummary;
import static org.covid19.bot.BotUtils.buildStateSummaryAlertText;
import static org.covid19.bot.BotUtils.buildSummaryAlertBlock;
import static org.covid19.bot.BotUtils.sendTelegramAlert;
import static org.covid19.bot.BotUtils.sendTelegramAlertWithPhoto;

@Configuration
@Slf4j
public class UserRequestConsumer {
    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;
    private final Covid19Bot covid19Bot;
    private final StateStoresManager stateStores;

    private static String TOTAL = "Total";

    public UserRequestConsumer(KafkaProperties kafkaProperties, Covid19Bot covid19Bot, StateStoresManager stateStores) {
        this.kafkaProperties = kafkaProperties;
        this.covid19Bot = covid19Bot;
        this.stateStores = stateStores;
        this.stringSerde = Serdes.String();
    }

    // consumer setup for user requests
    @Bean
    public Map<String, Object> userRequestsConsumerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(GROUP_ID_CONFIG, "org.covid19.patient-telegram-bot-user-requests-consumer");
        props.put(CLIENT_ID_CONFIG, "org.covid19.patient-telegram-bot-user-requests-consumer-client");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, stringSerde.deserializer().getClass().getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(JSON_VALUE_TYPE, UserRequest.class);
        return props;
    }

    @Bean
    public ConsumerFactory<String, UserRequest> userRequestsConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(userRequestsConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UserRequest> userRequestsKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, UserRequest> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(userRequestsConsumerFactory());
        factory.setMissingTopicsFatal(false);
        return factory;
    }

    @KafkaListener(topics = "user-request", id = "userRequestsConsumer",
            idIsGroup = false, autoStartup = "false",
            containerFactory = "userRequestsKafkaListenerContainerFactory")
    public void listenForUserRequests(@Payload UserRequest request) {
        if ("Summary".equalsIgnoreCase(request.getState())) {
            sendTelegramAlert(covid19Bot, request.getChatId(), buildStateSummary(false, stateStores), null, true);
            return;
        }
        if ("Today".equalsIgnoreCase(request.getState())) {
            sendTelegramAlert(covid19Bot, request.getChatId(), buildStateSummary(true, stateStores), null, true);
            return;
        }
        if ("Yesterday".equalsIgnoreCase(request.getState())) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
            String yesterday = dateTimeFormatter.format(Instant.now().minus(1, DAYS));
            sendTelegramAlert(covid19Bot, request.getChatId(), buildSpecificDateSummary(yesterday), null, true);
            return;
        }

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
        String yesterday = dateTimeFormatter.format(Instant.now().minus(1, DAYS));

        StatewiseDelta delta = stateStores.deltaStatsForState(request.getState());
        StatewiseDelta daily = stateStores.dailyStatsForState(request.getState());
        String newsSource = stateStores.newsSourceFor(request.getState());

        Map<String, StatewiseTestData> testing = new HashMap<>();
        final StatewiseTestData statewiseTestData = stateStores.latestAvailableTestDataFor(request.getState());
        if (nonNull(statewiseTestData)) {
            LOG.info("Found testing data for {} as {}", request.getState(), statewiseTestData);
            testing.put(request.getState(), statewiseTestData);
        }

        Map<String, String> doublingRates = new HashMap<>();
        doublingRates.put(request.getState(), stateStores.doublingRateFor(request.getState(), yesterday));

        List<DistrictwiseData> districtwiseData = stateStores.districtDailyStatsFor(request.getState());
        districtwiseData.sort((o1, o2) -> parseInt(o2.getDeltaConfirmed()) - parseInt(o1.getDeltaConfirmed()));
        Map<String, List<DistrictwiseData>> districtsData = new HashMap<>();
        districtsData.put(request.getState(), districtwiseData);

        AtomicReference<String> alertText = new AtomicReference<>("");
        buildSummaryAlertBlock(alertText, singletonList(delta), singletonList(daily), testing, doublingRates, districtsData);
        if (!TOTAL.equalsIgnoreCase(request.getState())) {
            alertText.accumulateAndGet(newsSource, (current, update) -> current + "\nSource: " + update);
        }
        if (!testing.isEmpty() && testing.containsKey(request.getState())) {
            alertText.accumulateAndGet(fetchStateNewsSource(testing.get(request.getState())), (current, update) -> current + "\n\nTesting data source: " + update);
        }

        byte[] stateTotalChart = stateStores.statewiseTotalChart(request.getState());

        sendTelegramAlertWithPhoto(covid19Bot, request.getChatId(), alertText.get(), null, true, request.getState(), stateTotalChart);
    }

    private String buildSpecificDateSummary(String date) {
        final List<StatewiseDelta> stats = stateStores.dailyCountFor(date);
        stats.sort((o1, o2) -> (int) (o2.getDeltaConfirmed() - o1.getDeltaConfirmed()));

        String lastUpdated = stats.get(0).getLastUpdatedTime();

        return buildStateSummaryAlertText(stats, lastUpdated, true);
    }

    private static String fetchStateNewsSource(StatewiseTestData data) {
        if (nonNull(data.getSource()) && !data.getSource().isEmpty()) {
            return data.getSource();
        }
        if (nonNull(data.getSource1()) && !data.getSource1().isEmpty()) {
            return data.getSource1();
        }
        if (nonNull(data.getSource2()) && !data.getSource2().isEmpty()) {
            return data.getSource2();
        }
        return "Not available";
    }
}
