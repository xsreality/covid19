package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KTable;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.covid19.TelegramUtils.buildStatewiseAlertText;
import static org.covid19.TelegramUtils.fireStatewiseTelegramAlert;
import static org.covid19.Utils.friendlyTime;

@EnableKafka
@Configuration
public class StatsAlertConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;
    private final Covid19Bot covid19Bot;
    private ReadOnlyKeyValueStore<String, StatewiseDelta> dailyStatsStore;
    private KafkaListenerEndpointRegistry registry;

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
}