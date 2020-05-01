package org.covid19;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.covid19.bot.Covid19Bot;
import org.covid19.district.DistrictwiseData;
import org.springframework.beans.factory.annotation.Value;
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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import static io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;
import static java.time.ZoneId.of;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.covid19.Utils.friendlyTime;
import static org.covid19.bot.BotUtils.buildStateSummary;
import static org.covid19.bot.BotUtils.buildStatewiseAlertText;
import static org.covid19.bot.BotUtils.fireStatewiseTelegramAlert;
import static org.covid19.bot.BotUtils.sendTelegramAlert;

@EnableKafka
@Configuration
@EnableScheduling
@Slf4j
public class StatsAlertConsumerConfig {
    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;
    private final Covid19Bot covid19Bot;
    private final StateStoresManager stateStores;
    private static String TOTAL = "Total";

    @Value("${telegram.tester.id}")
    private String telegramTestUser;

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

        // used to fetch doubling rate of yesterday (today's figure cannot be used as they are not yet final)
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
        String yesterday = dateTimeFormatter.format(Instant.now().minus(1, DAYS));

        List<StatewiseDelta> readyToSend = new ArrayList<>();
        List<StatewiseDelta> dailyIncrements = new ArrayList<>();
        Map<String, String> doublingRates = new HashMap<>();
        String lastUpdated = deltas.get(deltas.size() - 1).getLastUpdatedTime();

        for (StatewiseDelta delta : deltas) {
            if (TOTAL.equalsIgnoreCase(delta.getState())) {
                lastUpdated = delta.getLastUpdatedTime();
            }
            if (delta.getDeltaRecovered() < 1L && delta.getDeltaConfirmed() < 1L && delta.getDeltaDeaths() < 1L) {
                continue;
            }
            readyToSend.add(delta);
            dailyIncrements.add(stateStores.dailyStatsForState(delta.getState()));
            doublingRates.put(delta.getState(), stateStores.doublingRateFor(delta.getState(), yesterday));
        }
        if (readyToSend.isEmpty()) {
            return; // no useful update
        }
        // when alerting for multiple states, never include testing
        // or district data to avoid too big a message (overload).
        String alertTextForAllStates = buildStatewiseAlertText(
                friendlyTime(lastUpdated), readyToSend,
                dailyIncrements, emptyMap(), doublingRates, emptyList());

        final KeyValueIterator<String, UserPrefs> userPrefsIterator = stateStores.userPrefs();
        userPrefsIterator.forEachRemaining(keyValue -> {
            UserPrefs userPref = keyValue.value;
            if (!userPref.isSubscribed()) {
                LOG.info("Skipping user {} because unsubscribed", userPref.getUserId());
                return;
            }
            if (userPref.getMyStates().isEmpty()) {
                LOG.info("User {} has no preferred state. Sending alert...", userPref.getUserId());
                // send alert for all states
                sendTelegramAlert(covid19Bot, userPref.getUserId(), alertTextForAllStates, null, true);
                return;
            }

            // user has a preferred state
            List<StatewiseDelta> userStatesDelta = new ArrayList<>();
            List<StatewiseDelta> userStatesDaily = new ArrayList<>();
            List<DistrictwiseData> userDistrictDelta = new ArrayList<>();
            Map<String, String> userDoublingRates = new HashMap<>();
            String lastUpdatedUserState = deltas.get(deltas.size() - 1).getLastUpdatedTime();
            boolean userHasRelevantUpdate = false;
            for (StatewiseDelta delta : deltas) {
                if (TOTAL.equalsIgnoreCase(delta.getState())) {
                    lastUpdatedUserState = delta.getLastUpdatedTime();
                    userStatesDelta.add(stateStores.deltaStatsForState(TOTAL));
                    userStatesDaily.add(stateStores.dailyStatsForState(TOTAL));
                    userDoublingRates.put(delta.getState(), stateStores.doublingRateFor(delta.getState(), yesterday));
                    continue;
                }
                if (delta.getDeltaRecovered() < 1L && delta.getDeltaConfirmed() < 1L && delta.getDeltaDeaths() < 1L) {
                    continue;
                }
                if (userPref.getMyStates().contains(delta.getState())) {
                    // update available for user's preferred state
                    userHasRelevantUpdate = true;
                    userStatesDelta.add(stateStores.deltaStatsForState(delta.getState()));
                    userStatesDaily.add(stateStores.dailyStatsForState(delta.getState()));
                    userDistrictDelta.addAll(stateStores.districtDeltaStatsFor(delta.getState()));
                    userDoublingRates.put(delta.getState(), stateStores.doublingRateFor(delta.getState(), yesterday));
                }
            }
            if (!userHasRelevantUpdate) {
                LOG.info("Update not relevant for user {}", userPref.getUserId());
                return; // no useful update for this user
            }
            LOG.info("Update is relevant for user {}", userPref.getUserId());

            String alertTextForUserStates;
            alertTextForUserStates = buildStatewiseAlertText(friendlyTime(lastUpdatedUserState), userStatesDelta, userStatesDaily, emptyMap(), userDoublingRates, userDistrictDelta);
            sendTelegramAlert(covid19Bot, userPref.getUserId(), alertTextForUserStates, null, true);
        });
    }

    @Scheduled(cron = "0 20 4,8,12,16,18 * * ?")
    public void sendSummaryUpdates() {
        String text = buildStateSummary(false, stateStores);

        LOG.info("summary text {}", text);
        fireStatewiseTelegramAlert(covid19Bot, text);
    }
}