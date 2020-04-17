package org.covid19;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class StateStoresManager {
    private ReadOnlyKeyValueStore<String, StatewiseDelta> dailyStatsStore;
    private ReadOnlyKeyValueStore<String, StatewiseDelta> deltaStatsStore;
    private ReadOnlyKeyValueStore<String, UserPrefs> userPrefsStore;

    private KafkaListenerEndpointRegistry registry;

    public StateStoresManager(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
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
    public ApplicationRunner runner(StreamsBuilderFactoryBean fb,
                                    KTable<String, StatewiseDelta> dailyStatsTable,
                                    KTable<String, StatewiseDelta> deltaStatsTable,
                                    KTable<String, UserPrefs> userPrefsTable) {
        return args -> {
            latch(fb).await(100, TimeUnit.SECONDS);
            dailyStatsStore = fb.getKafkaStreams().store(dailyStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            deltaStatsStore = fb.getKafkaStreams().store(deltaStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            userPrefsStore = fb.getKafkaStreams().store(userPrefsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            // start consumers only after state store is ready.
            registry.getListenerContainer("statewiseAlertsConsumer").start();
            registry.getListenerContainer("userRequestsConsumer").start();
        };
    }

    public KeyValueIterator<String, StatewiseDelta> dailyStats() {
        return dailyStatsStore.all();
    }

    public KeyValueIterator<String, StatewiseDelta> deltaStats() {
        return deltaStatsStore.all();
    }

    public KeyValueIterator<String, UserPrefs> userPrefs() {
        return userPrefsStore.all();
    }

    public StatewiseDelta dailyStatsForState(String state) {
        return dailyStatsStore.get(state);
    }

    public StatewiseDelta deltaStatsForState(String state) {
        return deltaStatsStore.get(state);
    }
}
