package org.covid19;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

import static io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.covid19.TelegramUtils.buildStateSummaryAlertText;
import static org.covid19.TelegramUtils.buildStatewiseAlertText;
import static org.covid19.TelegramUtils.buildSummaryAlertBlock;
import static org.covid19.TelegramUtils.fireStatewiseTelegramAlert;
import static org.covid19.TelegramUtils.sendTelegramAlert;
import static org.covid19.Utils.friendlyTime;

@EnableKafka
@Configuration
@EnableScheduling
@Slf4j
public class StatsAlertConsumerConfig {
    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;
    private final Covid19Bot covid19Bot;
    private final StateStoresManager stateStores;

    public StatsAlertConsumerConfig(KafkaProperties kafkaProperties, Covid19Bot covid19Bot, StateStoresManager stateStores) {
        this.kafkaProperties = kafkaProperties;
        this.covid19Bot = covid19Bot;
        this.stateStores = stateStores;
        this.stringSerde = Serdes.String();
    }

    // consumer setup for statewise alerts
    @Bean
    public Map<String, Object> statewiseAlertsConsumerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, stringSerde.deserializer().getClass().getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(JSON_VALUE_TYPE, StatewiseDelta.class);
        return props;
    }

    @Bean
    public ConsumerFactory<String, StatewiseDelta> statewiseAlertsConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(statewiseAlertsConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StatewiseDelta> statewiseAlertsKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StatewiseDelta> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(statewiseAlertsConsumerFactory());
        factory.setMissingTopicsFatal(false);
        factory.setBatchListener(true);
        return factory;
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


    @KafkaListener(topics = "statewise-delta-stats", id = "statewiseAlertsConsumer",
            idIsGroup = false, autoStartup = "false",
            containerFactory = "statewiseAlertsKafkaListenerContainerFactory")
    public void listenDeltaStats(@Payload List<StatewiseDelta> deltas) {
        try {
            // strategic delay to allow the other topology to process
            // records before this topology completes. This allows
            // the statewise daily incremental stats to be calculated before
            // the statewise delta stats are calculated. The consumer listening
            // on the statewise-delta-stats will be triggered after daily stats have
            // been updated in the KTable.
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignore
        }
        List<StatewiseDelta> readyToSend = new ArrayList<>();
        List<StatewiseDelta> dailyIncrements = new ArrayList<>();
        String lastUpdated = deltas.get(deltas.size() - 1).getLastUpdatedTime();
        for (StatewiseDelta delta : deltas) {
            if ("Total".equalsIgnoreCase(delta.getState())) {
                lastUpdated = delta.getLastUpdatedTime();
            }
            if (delta.getDeltaRecovered() < 1L && delta.getDeltaConfirmed() < 1L && delta.getDeltaDeaths() < 1L) {
                continue;
            }
            readyToSend.add(delta);
            dailyIncrements.add(stateStores.dailyStatsForState(delta.getState()));
        }
        if (readyToSend.isEmpty()) {
            return;
        }
        fireStatewiseTelegramAlert(covid19Bot, buildStatewiseAlertText(friendlyTime(lastUpdated), readyToSend, dailyIncrements));
        readyToSend.clear();
    }

    @KafkaListener(topics = "user-request", id = "userRequestsConsumer",
            idIsGroup = false, autoStartup = "false",
            containerFactory = "userRequestsKafkaListenerContainerFactory")
    public void listenForUserRequests(@Payload UserRequest request) {
        if ("Summary".equalsIgnoreCase(request.getState())) {
            sendTelegramAlert(covid19Bot, request.getChatId(), buildStateSummary(), null, true);
            return;
        }
        StatewiseDelta delta = stateStores.deltaStatsForState(request.getState());
        StatewiseDelta daily = stateStores.dailyStatsForState(request.getState());
        AtomicReference<String> alertText = new AtomicReference<>("");
        buildSummaryAlertBlock(alertText, singletonList(delta), singletonList(daily));
        sendTelegramAlert(covid19Bot, request.getChatId(), alertText.get(), null, true);
    }

    @Scheduled(cron = "0 20 4,8,12,16,18 * * ?")
    public void sendSummaryUpdates() {
        String text = buildStateSummary();

        LOG.info("summary text {}", text);
        fireStatewiseTelegramAlert(covid19Bot, text);
    }

    private String buildStateSummary() {
        final KeyValueIterator<String, StatewiseDelta> stats = stateStores.dailyStats();

        List<StatewiseDelta> sortedStats = new ArrayList<>();
        StatewiseDelta total = new StatewiseDelta();
        stats.forEachRemaining(stat -> sortedStats.add(stat.value));
        sortedStats.sort((o1, o2) -> (int) (o2.getCurrentConfirmed() - o1.getCurrentConfirmed()));

        String lastUpdated = sortedStats.get(0).getLastUpdatedTime();

        return buildStateSummaryAlertText(sortedStats, total, lastUpdated);
    }
}