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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import static java.time.ZoneId.of;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Objects.isNull;
import static org.covid19.visualizations.Visualizer.LAST_SEVEN_DAYS_OVERVIEW;

@Slf4j
@Configuration
public class StateStoresManager {
    private ReadOnlyKeyValueStore<String, StatewiseDelta> dailyStatsStore;
    private ReadOnlyKeyValueStore<String, StatewiseDelta> deltaStatsStore;
    private ReadOnlyKeyValueStore<String, UserPrefs> userPrefsStore;
    private ReadOnlyKeyValueStore<String, String> newsSourcesStore;
    private ReadOnlyKeyValueStore<StateAndDate, String> doublingRateStore;
    private ReadOnlyKeyValueStore<StateAndDate, StatewiseDelta> dailyCountStore;
    private ReadOnlyKeyValueStore<StateAndDate, StatewiseTestData> stateTestStore;
    private ReadOnlyKeyValueStore<String, byte[]> visualizationsStore;

    private KafkaListenerEndpointRegistry registry;
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));

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
                                    KTable<StateAndDate, StatewiseDelta> dailyCountTable,
                                    KTable<StateAndDate, StatewiseTestData> stateTestTable,
                                    KTable<String, byte[]> visualizationsTable) {
        return args -> {
            latch(fb).await(100, TimeUnit.SECONDS);
            dailyStatsStore = fb.getKafkaStreams().store(dailyStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            deltaStatsStore = fb.getKafkaStreams().store(deltaStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            userPrefsStore = fb.getKafkaStreams().store(userPrefsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            newsSourcesStore = fb.getKafkaStreams().store(newsSourcesTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            newsSourcesStore = fb.getKafkaStreams().store(newsSourcesTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            doublingRateStore = fb.getKafkaStreams().store(doublingRateTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            dailyCountStore = fb.getKafkaStreams().store(dailyCountTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            stateTestStore = fb.getKafkaStreams().store(stateTestTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            visualizationsStore = fb.getKafkaStreams().store(visualizationsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
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

    public StatewiseTestData testDataFor(String state, String date) {
        return stateTestStore.get(new StateAndDate(date, state));
    }

    /**
     * Iterate through the testing data store from today in reverse chronological order until data
     * is found within the past 14 days.
     *
     * @param state Indian state to search testing data for
     * @return Testing data
     */
    public StatewiseTestData latestAvailableTestDataFor(String state) {
        StatewiseTestData testData;
        long deltaDays = 0L;
        do {
            testData = testDataFor(state, dateTimeFormatter.format(Instant.now().minus(deltaDays++, DAYS)));
            if (deltaDays >= 14L) {
                break; // don't bother beyond 2 weeks
            }
        } while (isNull(testData));
        return testData;
    }

    public byte[] last7DaysOverview() {
        return visualizationsStore.get(LAST_SEVEN_DAYS_OVERVIEW);
    }
}
