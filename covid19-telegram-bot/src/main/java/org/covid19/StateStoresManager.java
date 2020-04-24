package org.covid19;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class StateStoresManager {
    private ReadOnlyKeyValueStore<String, StatewiseDelta> dailyStatsStore;
    private ReadOnlyKeyValueStore<String, StatewiseDelta> deltaStatsStore;
    private ReadOnlyKeyValueStore<String, UserPrefs> userPrefsStore;
    private ReadOnlyKeyValueStore<String, String> newsSourcesStore;
    private ReadOnlyKeyValueStore<StateAndDate, String> doublingRateStore;
    private ReadOnlyKeyValueStore<StateAndDate, StatewiseDelta> dailyCountStore;

    private KafkaListenerEndpointRegistry registry;

    public StateStoresManager(@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection") KafkaListenerEndpointRegistry registry) {
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
                                    KTable<String, UserPrefs> userPrefsTable,
                                    KTable<String, String> newsSourcesTable,
                                    KTable<StateAndDate, String> doublingRateTable,
                                    KTable<StateAndDate, StatewiseDelta> dailyCountTable) {
        return args -> {
            latch(fb).await(100, TimeUnit.SECONDS);
            dailyStatsStore = fb.getKafkaStreams().store(dailyStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            deltaStatsStore = fb.getKafkaStreams().store(deltaStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            userPrefsStore = fb.getKafkaStreams().store(userPrefsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            newsSourcesStore = fb.getKafkaStreams().store(newsSourcesTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            newsSourcesStore = fb.getKafkaStreams().store(newsSourcesTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            doublingRateStore = fb.getKafkaStreams().store(doublingRateTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            dailyCountStore = fb.getKafkaStreams().store(dailyCountTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
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

    public UserPrefs prefsForUser(String userId) {
        return userPrefsStore.get(userId);
    }

    public String newsSourceFor(String state) {
        return newsSourcesStore.get(state);
    }

    public String doublingRateFor(String state, String date) {
        return doublingRateStore.get(new StateAndDate(date, state));
    }

    public StatewiseDelta dailyCountFor(String state, String date) {
        return dailyCountStore.get(new StateAndDate(date, state));
    }

    public List<StatewiseDelta> dailyCountFor(String date) {
        List<StatewiseDelta> counts = new ArrayList<>();
        final KeyValueIterator<StateAndDate, StatewiseDelta> iterator = dailyCountStore.all();
        while (iterator.hasNext()) {
            final KeyValue<StateAndDate, StatewiseDelta> count = iterator.next();
            if (count.key.getDate().equalsIgnoreCase(date)) {
                counts.add(count.value);
            }
        }
        return counts;
    }
}
