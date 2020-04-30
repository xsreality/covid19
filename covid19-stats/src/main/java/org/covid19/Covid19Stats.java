package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.covid19.district.DistrictAndDate;
import org.covid19.district.DistrictAndDateSerde;
import org.covid19.district.DistrictwiseData;
import org.covid19.district.DistrictwiseDataSerde;
import org.covid19.district.StateAndDistrict;
import org.covid19.district.StateAndDistrictSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static java.lang.Long.parseLong;
import static java.lang.Math.round;
import static java.lang.String.valueOf;
import static java.time.Duration.ofDays;
import static java.time.ZoneId.of;
import static java.util.Objects.isNull;

public class Covid19Stats {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Stats.class);

    private static String KAFKA_GLOBAL_STATS_APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String KAFKA_GLOBAL_STATS_CLIENT_ID;
    private static String STATE_DIR;

    public static void main(final String[] args) throws InterruptedException {
        initEnv();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GLOBAL_STATS_APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, KAFKA_GLOBAL_STATS_CLIENT_ID);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 20 * 1024 * 1024L);
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
        DecimalFormat decimalFormatter = new DecimalFormat("0");

        final Serde<String> stringSerde = Serdes.String();
        final Serde<StateAndDistrict> stateAndDistrictSerde = new StateAndDistrictSerde();
        final Serde<DistrictwiseData> districtwiseDataSerde = new DistrictwiseDataSerde();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<StateAndDate> stateAndDateSerde = new StateAndDateSerde();
        final Serde<StatewiseStats> statewiseStatsSerde = new StatewiseStatsSerde();
        final Serde<StatewiseDelta> statewiseDeltaSerde = new StatewiseDeltaSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        // build the statewise delta stats of recovery, death and confirmed Covid19 cases
        // This KTable is read by covid19-telegram-bot to send statewise delta updates
        builder.table("statewise-data",
                Materialized.<String, StatewiseStats, KeyValueStore<Bytes, byte[]>>as("ignored")
                        .withKeySerde(stringSerde).withValueSerde(statewiseStatsSerde)
                        .withLoggingDisabled())
                .groupBy(KeyValue::pair, Grouped.with(stringSerde, statewiseStatsSerde))
                .aggregate(StatewiseDelta::new, Covid19Stats::calculateDeltaStats,
                        (state, oldStatewiseStats, aggregate) -> aggregate,
                        Materialized.<String, StatewiseDelta, KeyValueStore<Bytes, byte[]>>as("statewise-delta")
                                .withKeySerde(stringSerde)
                                .withValueSerde(statewiseDeltaSerde))
                .toStream()
                .to("statewise-delta-stats", Produced.with(stringSerde, statewiseDeltaSerde));


        // we calculate delta ourselves instead of relying on data from API
        builder.table("districtwise-data",
                Materialized.<StateAndDistrict, DistrictwiseData, KeyValueStore<Bytes, byte[]>>as("district-ignored")
                        .withKeySerde(stateAndDistrictSerde).withValueSerde(districtwiseDataSerde)
                        .withLoggingDisabled())
                .groupBy(KeyValue::pair, Grouped.with(stateAndDistrictSerde, districtwiseDataSerde))
                .aggregate(() -> new DistrictwiseData("", "", "0", "0", "0", "0", "0", "0", "0", ""),
                        Covid19Stats::calculateDistrictDelta,
                        (key, value, aggregate) -> aggregate,
                        Materialized.<StateAndDistrict, DistrictwiseData, KeyValueStore<Bytes, byte[]>>as("districtwise-delta")
                                .withKeySerde(stateAndDistrictSerde).withValueSerde(districtwiseDataSerde))
                .toStream()
                .to("districtwise-delta", Produced.with(stateAndDistrictSerde, districtwiseDataSerde));


        final KTable<Windowed<StateAndDistrict>, DistrictwiseData> districtwiseWindowedTable =
                builder.stream("org.covid19.patient-status-stats-districtwise-delta-changelog",
                        Consumed.with(stateAndDistrictSerde, districtwiseDataSerde))
                        .groupByKey(Grouped.with(stateAndDistrictSerde, districtwiseDataSerde))
                        .windowedBy(TimeWindows.of(ofDays(1L)))
                        .aggregate(() -> new DistrictwiseData("", "", "0", "0", "0", "0", "0", "0", "0", ""),
                                Covid19Stats::calculateDistrictDaily,
                                Materialized.<StateAndDistrict, DistrictwiseData, WindowStore<Bytes, byte[]>>as("districtwise-windowed-daily")
                                        .withKeySerde(stateAndDistrictSerde).withValueSerde(districtwiseDataSerde)
                                        .withCachingDisabled().withRetention(ofDays(365L)));

        // store district -> daily stats in a stream
        districtwiseWindowedTable
                .toStream()
                .selectKey((key, value) -> key.key())
                .to("districtwise-daily", Produced.with(stateAndDistrictSerde, districtwiseDataSerde));

        districtwiseWindowedTable
                .toStream()
                .selectKey((key, value) -> new DistrictAndDate(dateTimeFormatter.format(key.window().startTime()), key.key().getState(), key.key().getDistrict()))
                .to("daily-district-count", Produced.with(new DistrictAndDateSerde(), districtwiseDataSerde));

        // topology to build daily incremental stats
        final KTable<Windowed<String>, StatewiseDelta> statewiseWindowedTable =
                builder.stream("org.covid19.patient-status-stats-statewise-delta-changelog",
                        Consumed.with(stringSerde, statewiseDeltaSerde))
                        .groupByKey(Grouped.with(stringSerde, statewiseDeltaSerde))
                        .windowedBy(TimeWindows.of(ofDays(1L)))
                        .aggregate(StatewiseDelta::new, (state, newDelta, aggregate) -> calculateDailyIncrements(newDelta, aggregate),
                                Materialized.<String, StatewiseDelta, WindowStore<Bytes, byte[]>>as("statewise-windowed-daily")
                                        .withKeySerde(stringSerde)
                                        .withValueSerde(statewiseDeltaSerde)
                                        .withCachingDisabled()
                                        .withRetention(ofDays(365L)));

        // store state -> daily stats in a stream
        statewiseWindowedTable
                .toStream()
                .selectKey((windowedKey, delta) -> windowedKey.key())
                .to("statewise-daily-stats", Produced.with(stringSerde, statewiseDeltaSerde))
        ;

        // store state + window -> daily stats in a stream
        statewiseWindowedTable
                .toStream()
                .selectKey((key, value) -> new StateAndDate(dateTimeFormatter.format(key.window().startTime()), key.key()))
                .to("daily-states-count", Produced.with(stateAndDateSerde, statewiseDeltaSerde))
        ;

        // calculate doubling rate and store in a stream
        builder.stream("daily-states-count", Consumed.with(stateAndDateSerde, statewiseDeltaSerde))
                .mapValues((readOnlyKey, statewiseDelta) -> calculateDoublingRate(statewiseDelta, decimalFormatter))
                .to("doubling-rate", Produced.with(stateAndDateSerde, stringSerde))
        ;

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        LOG.info("{}", builder.build().describe());

        streams.start();

        final ReadOnlyWindowStore<String, StatewiseDelta> dailyWindowStore =
                waitUntilStoreIsQueryable("statewise-windowed-daily", QueryableStoreTypes.windowStore(), streams);

        // Fetch values for the key "world" for all of the windows available in this application instance.
// To get *all* available windows we fetch windows from the beginning of time until now.
//        Instant timeFrom = Instant.ofEpochMilli(0); // beginning of time = oldest available
//        Instant timeTo = Instant.now(); // now (in processing-time)
//        WindowStoreIterator<StatewiseDelta> iterator = dailyWindowStore.fetch("Total", timeFrom, timeTo);
//        while (iterator.hasNext()) {
//            KeyValue<Long, StatewiseDelta> next = iterator.next();
//
//            long windowTimestamp = next.key;
//            System.out.println("Count of 'Total' @ time " + windowTimestamp + " is " + next.value);
//        }
//// close the iterator to release resources
//        iterator.close();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String calculateDoublingRate(StatewiseDelta statewiseDelta, DecimalFormat decimalFormatter) {
        if (statewiseDelta.getDeltaConfirmed() == 0L || statewiseDelta.getCurrentConfirmed() == 0L) {
            return "0";
        }
        final double growthPercent = 100.0 * statewiseDelta.getDeltaConfirmed() / statewiseDelta.getCurrentConfirmed();
        final double doublingRate = 70.0 / growthPercent;
        return decimalFormatter.format(round(doublingRate));
    }

    private static boolean isToday(StateAndDate stateAndDate) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
        String today = formatter.format(Instant.now());
        return today.equalsIgnoreCase(stateAndDate.getDate());
    }

    private static DistrictwiseData calculateDistrictDelta(StateAndDistrict stateAndDistrict, DistrictwiseData newData, DistrictwiseData aggregate) {
        // update deltas
        aggregate.setDeltaConfirmed(valueOf(parseLong(newData.getConfirmed()) - parseLong(aggregate.getConfirmed())));
        aggregate.setDeltaRecovered(valueOf(parseLong(newData.getRecovered()) - parseLong(aggregate.getRecovered())));
        aggregate.setDeltaDeceased(valueOf(parseLong(newData.getDeceased()) - parseLong(aggregate.getDeceased())));

        aggregate.setConfirmed(newData.getConfirmed());
        aggregate.setRecovered(newData.getRecovered());
        aggregate.setDeceased(newData.getDeceased());

        // update metadata
        aggregate.setState(newData.getState());
        aggregate.setDistrict(newData.getDistrict());
        return aggregate;
    }

    private static StatewiseDelta calculateDeltaStats(String state, StatewiseStats newStatewiseStats, StatewiseDelta aggregate) {
        // update deltas
        aggregate.setDeltaRecovered(parseLong(newStatewiseStats.getRecovered()) - aggregate.getCurrentRecovered());
        aggregate.setDeltaDeaths(parseLong(newStatewiseStats.getDeaths()) - aggregate.getCurrentDeaths());
        aggregate.setDeltaConfirmed(parseLong(newStatewiseStats.getConfirmed()) - aggregate.getCurrentConfirmed());

        // update current values
        aggregate.setCurrentRecovered(parseLong(newStatewiseStats.getRecovered()));
        aggregate.setCurrentDeaths(parseLong(newStatewiseStats.getDeaths()));
        aggregate.setCurrentConfirmed(parseLong(newStatewiseStats.getConfirmed()));

        // update metadata
        aggregate.setLastUpdatedTime(newStatewiseStats.getLastUpdatedTime());
        aggregate.setState(state);
        return aggregate;
    }

    private static DistrictwiseData calculateDistrictDaily(StateAndDistrict stateAndDistrict, DistrictwiseData newData, DistrictwiseData aggregate) {
        aggregate.setDeltaConfirmed(valueOf(parseLong(aggregate.getDeltaConfirmed()) + parseLong(newData.getDeltaConfirmed())));
        aggregate.setDeltaRecovered(valueOf(parseLong(aggregate.getDeltaRecovered()) + parseLong(newData.getDeltaRecovered())));
        aggregate.setDeltaDeceased(valueOf(parseLong(aggregate.getDeltaDeceased()) + parseLong(newData.getDeltaDeceased())));

        aggregate.setConfirmed(newData.getConfirmed());
        aggregate.setRecovered(newData.getRecovered());
        aggregate.setDeceased(newData.getDeceased());

        aggregate.setState(newData.getState());
        aggregate.setDistrict(newData.getDistrict());
        return aggregate;
    }

    private static StatewiseDelta calculateDailyIncrements(StatewiseDelta newDelta, StatewiseDelta aggregate) {
        aggregate.setDeltaConfirmed(aggregate.getDeltaConfirmed() + newDelta.getDeltaConfirmed());
        aggregate.setDeltaRecovered(aggregate.getDeltaRecovered() + newDelta.getDeltaRecovered());
        aggregate.setDeltaDeaths(aggregate.getDeltaDeaths() + newDelta.getDeltaDeaths());

        aggregate.setCurrentConfirmed(newDelta.getCurrentConfirmed());
        aggregate.setCurrentRecovered(newDelta.getCurrentRecovered());
        aggregate.setCurrentDeaths(newDelta.getCurrentDeaths());

        aggregate.setState(newDelta.getState());
        aggregate.setLastUpdatedTime(newDelta.getLastUpdatedTime());
        return aggregate;
    }

    private static void initEnv() {
        if (isNull(System.getenv("BOOTSTRAP_SERVERS"))) {
            LOG.error("Environment variable BOOTSTRAP_SERVERS must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_GLOBAL_STATS_APPLICATION_ID"))) {
            LOG.error("Environment variable KAFKA_GLOBAL_STATS_APPLICATION_ID must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_STATE_DIR"))) {
            LOG.error("Environment variable KAFKA_STATE_DIR must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        KAFKA_GLOBAL_STATS_APPLICATION_ID = System.getenv("KAFKA_GLOBAL_STATS_APPLICATION_ID");
        KAFKA_GLOBAL_STATS_CLIENT_ID = KAFKA_GLOBAL_STATS_APPLICATION_ID + "-client";
        STATE_DIR = System.getenv("KAFKA_STATE_DIR");
    }

    private static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                   final QueryableStoreType<T> queryableStoreType,
                                                   final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                LOG.info("Store not yet open... {}", ignored.getMessage());
                Thread.sleep(1000);
            }
        }
    }
}
