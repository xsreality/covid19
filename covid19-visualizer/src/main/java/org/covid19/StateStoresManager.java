package org.covid19;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.covid19.district.DistrictwiseData;
import org.covid19.district.StateAndDistrict;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
    private ReadOnlyKeyValueStore<StateAndDistrict, DistrictwiseData> districtDailyStore;
    private ReadOnlyKeyValueStore<StateAndDistrict, DistrictwiseData> districtDeltaStore;
    private ReadOnlyKeyValueStore<StateAndDistrict, String> districtZonesStore;
    private ReadOnlyKeyValueStore<String, StatewiseDelta> deltaStatsStore;
    private ReadOnlyKeyValueStore<StateAndDate, String> doublingRateStore;
    private ReadOnlyKeyValueStore<StateAndDate, StatewiseDelta> dailyCountStore;
    private ReadOnlyKeyValueStore<StateAndDate, StatewiseTestData> stateTestStore;
    private ReadOnlyKeyValueStore<String, byte[]> visualizationsStore;

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
                                    KTable<StateAndDate, String> doublingRateTable,
                                    KTable<StateAndDate, StatewiseDelta> dailyCountTable,
                                    KTable<StateAndDate, StatewiseTestData> stateTestTable,
                                    KTable<String, byte[]> visualizationsTable,
                                    KTable<StateAndDistrict, DistrictwiseData> districtDailyTable,
                                    KTable<StateAndDistrict, DistrictwiseData> districtDeltaTable,
                                    KTable<StateAndDistrict, String> districtZonesTable) {
        return args -> {
            latch(fb).await(100, TimeUnit.SECONDS);
            dailyStatsStore = fb.getKafkaStreams().store(dailyStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            districtDailyStore = fb.getKafkaStreams().store(districtDailyTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            districtDeltaStore = fb.getKafkaStreams().store(districtDeltaTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            districtZonesStore = fb.getKafkaStreams().store(districtZonesTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            deltaStatsStore = fb.getKafkaStreams().store(deltaStatsTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            doublingRateStore = fb.getKafkaStreams().store(doublingRateTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            dailyCountStore = fb.getKafkaStreams().store(dailyCountTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
            stateTestStore = fb.getKafkaStreams().store(stateTestTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
        };
    }

    public KeyValueIterator<String, StatewiseDelta> dailyStats() {
        return dailyStatsStore.all();
    }

    public KeyValueIterator<String, StatewiseDelta> deltaStats() {
        return deltaStatsStore.all();
    }

    public StatewiseDelta dailyStatsForState(String state) {
        return dailyStatsStore.get(state);
    }

    public DistrictwiseData deltaStatsForStateAndDistrict(String state, String district) {
        return districtDeltaStore.get(new StateAndDistrict(state, district));
    }

    public List<DistrictwiseData> districtDeltaStatsFor(String state) {
        final KeyValueIterator<StateAndDistrict, DistrictwiseData> all = districtDeltaStore.all();
        List<DistrictwiseData> data = new ArrayList<>();
        while (all.hasNext()) {
            final KeyValue<StateAndDistrict, DistrictwiseData> next = all.next();
            if (!state.equalsIgnoreCase(next.key.getState())) {
                continue;
            }
            data.add(next.value);
        }
        return data;
    }

    public List<DistrictwiseData> districtDailyStatsFor(String state) {
        final KeyValueIterator<StateAndDistrict, DistrictwiseData> all = districtDailyStore.all();
        List<DistrictwiseData> data = new ArrayList<>();
        while (all.hasNext()) {
            final KeyValue<StateAndDistrict, DistrictwiseData> next = all.next();
            if (!state.equalsIgnoreCase(next.key.getState())) {
                continue;
            }
            data.add(next.value);
        }
        return data;
    }

    public DistrictwiseData dailyStatsForStateAndDistrict(String state, String district) {
        return districtDailyStore.get(new StateAndDistrict(state, district));
    }

    public String doublingRateFor(String state, String date) {
        return doublingRateStore.get(new StateAndDate(date, state));
    }

    public KeyValueIterator<StateAndDate, StatewiseDelta> dailyCount() {
        return dailyCountStore.all();
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
}
