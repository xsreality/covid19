package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import static org.covid19.TelegramUtils.buildStatewiseAlertText;
import static org.covid19.TelegramUtils.fireStatewiseTelegramAlert;
import static org.covid19.Utils.friendlyTime;
import static org.covid19.Utils.initStateCodes;

@EnableKafka
@Configuration
@EnableScheduling
@Slf4j
public class StatsAlertConsumerConfig {
    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;
    private final Covid19Bot covid19Bot;
    private ReadOnlyKeyValueStore<String, StatewiseDelta> dailyStatsStore;
    private KafkaListenerEndpointRegistry registry;
    final static Map<String, String> stateCodes;

    static {
        stateCodes = initStateCodes();
    }

    public StatsAlertConsumerConfig(KafkaProperties kafkaProperties, Covid19Bot covid19Bot, KafkaListenerEndpointRegistry registry) {
        this.kafkaProperties = kafkaProperties;
        this.covid19Bot = covid19Bot;
        this.registry = registry;
        this.stringSerde = Serdes.String();
    }


    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, stringSerde.deserializer().getClass().getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put("json.value.type", StatewiseDelta.class);
        return props;
    }

    @Bean
    public ConsumerFactory<String, StatewiseDelta> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StatewiseDelta> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StatewiseDelta> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setMissingTopicsFatal(false);
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public CountDownLatch latch(StreamsBuilderFactoryBean fb) {
        CountDownLatch latch = new CountDownLatch(1);
        fb.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.RUNNING.equals(newState)) {
                latch.countDown();
            }
        });
        return latch;
    }

    @Bean
    public ApplicationRunner runner(StreamsBuilderFactoryBean fb, KTable<String, StatewiseDelta> dailyStatsTable) {
        return args -> {
            latch(fb).await(100, TimeUnit.SECONDS);
            dailyStatsStore = fb.getKafkaStreams().store(dailyStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            registry.getListenerContainer("statewiseAlertsConsumer").start(); // start consumer only after state store is ready.
        };
    }

    @KafkaListener(topics = "statewise-delta-stats", id = "statewiseAlertsConsumer", idIsGroup = false, autoStartup = "false")
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
            dailyIncrements.add(dailyStatsStore.get(delta.getState()));
        }
        if (readyToSend.isEmpty()) {
            return;
        }
        fireStatewiseTelegramAlert(covid19Bot, buildStatewiseAlertText(friendlyTime(lastUpdated), readyToSend, dailyIncrements));
        readyToSend.clear();
    }

    @Scheduled(cron = "0 20 4,8,12,16,18 * * ?")
    public void sendSummaryUpdates() {
        final KeyValueIterator<String, StatewiseDelta> stats = dailyStatsStore.all();

        List<StatewiseDelta> sortedStats = new ArrayList<>();
        StatewiseDelta total = new StatewiseDelta();
        stats.forEachRemaining(stat -> sortedStats.add(stat.value));
        sortedStats.sort((o1, o2) -> (int) (o2.getCurrentConfirmed() - o1.getCurrentConfirmed()));

        String lastUpdated = sortedStats.get(0).getLastUpdatedTime();

        String text = String.format("<i>%s</i>\n\n", friendlyTime(lastUpdated));
        text = text.concat("Summary of all affected Indian States\n\n");
        text = text.concat("<pre>\n");
        text = text.concat("State|  Conf|  Rec.| Died\n");
        text = text.concat("-------------------------\n");
        for (StatewiseDelta stat : sortedStats) {
            if ("Total".equalsIgnoreCase(stat.getState())) {
                total = stat;
                continue; // show total at the end
            }
            if (stat.getCurrentConfirmed() < 1L && stat.getCurrentRecovered() < 1L && stat.getCurrentDeaths() < 1L) {
                continue; // skip states with zero stats
            }
            text = text.concat(String.format("%-5s|%6s|%6s|%5s\n", stateCodes.get(stat.getState()), stat.getCurrentConfirmed(), stat.getCurrentRecovered(), stat.getCurrentDeaths()));
        }
        text = text.concat("-------------------------\n");
        text = text.concat(String.format("%-5s|%6s|%6s|%5s\n", stateCodes.get(total.getState()), total.getCurrentConfirmed(), total.getCurrentRecovered(), total.getCurrentDeaths()));
        text = text.concat("</pre>");

        LOG.info("summary text {}", text);
        fireStatewiseTelegramAlert(covid19Bot, text);
    }
}