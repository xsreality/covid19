package org.covid19;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static java.lang.Long.parseLong;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Covid19Stats {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Stats.class);

    private static String KAFKA_GLOBAL_STATS_APPLICATION_ID;
    private static String BOOTSTRAP_SERVERS;
    private static String KAFKA_GLOBAL_STATS_CLIENT_ID;
    private static String KAFKA_TOPIC_PATIENT_STATUS_COUNT;
    private static String KAFKA_TOPIC_PATIENT_ALERTS;

    public static void main(final String[] args) {
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
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<PatientAndMessage> patientAndMessageSerde = new PatientAndMessageSerde();
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
                .aggregate(StatewiseDelta::new, (state, newStatewiseStats, aggregate) -> {
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
                        }, (state, oldStatewiseStats, aggregate) -> aggregate,
                        Materialized.<String, StatewiseDelta, KeyValueStore<Bytes, byte[]>>as("statewise-delta")
                                .withKeySerde(stringSerde)
                                .withValueSerde(statewiseDeltaSerde))
                .toStream()
                .to("statewise-delta-stats", Produced.with(stringSerde, statewiseDeltaSerde));


        // topology to build daily incremental stats
        builder.stream("org.covid19.patient-status-stats-statewise-delta-changelog",
                Consumed.with(stringSerde, statewiseDeltaSerde))
                .groupByKey(Grouped.with(stringSerde, statewiseDeltaSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
                .aggregate(StatewiseDelta::new, (state, newDelta, aggregate) -> {
                    aggregate.setDeltaConfirmed(aggregate.getDeltaConfirmed() + newDelta.getDeltaConfirmed());
                    aggregate.setDeltaRecovered(aggregate.getDeltaRecovered() + newDelta.getDeltaRecovered());
                    aggregate.setDeltaDeaths(aggregate.getDeltaDeaths() + newDelta.getDeltaDeaths());

                    aggregate.setCurrentConfirmed(newDelta.getCurrentConfirmed());
                    aggregate.setCurrentRecovered(newDelta.getCurrentRecovered());
                    aggregate.setCurrentDeaths(newDelta.getCurrentDeaths());

                    aggregate.setState(newDelta.getState());
                    aggregate.setLastUpdatedTime(newDelta.getLastUpdatedTime());
                    return aggregate;
                }, Materialized.<String, StatewiseDelta, WindowStore<Bytes, byte[]>>as("statewise-windowed-daily")
                        .withKeySerde(stringSerde)
                        .withValueSerde(statewiseDeltaSerde)
                        .withCachingDisabled())
                .toStream()
                .map((Windowed<String> key, StatewiseDelta delta) -> new KeyValue<>(key.key(), delta))
                .to("statewise-daily-stats", Produced.with(stringSerde, statewiseDeltaSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        LOG.info("{}", builder.build().describe());

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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
        if (isNull(System.getenv("KAFKA_TOPIC_PATIENT_ALERTS"))) {
            LOG.error("Environment variable KAFKA_TOPIC_PATIENT_ALERTS must be set!");
            System.exit(-1);
        }
        if (isNull(System.getenv("KAFKA_TOPIC_PATIENT_STATUS_COUNT"))) {
            LOG.error("Environment variable KAFKA_TOPIC_PATIENT_STATUS_COUNT must be set!");
            System.exit(-1);
        }

        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        KAFKA_GLOBAL_STATS_APPLICATION_ID = System.getenv("KAFKA_GLOBAL_STATS_APPLICATION_ID");
        KAFKA_GLOBAL_STATS_CLIENT_ID = KAFKA_GLOBAL_STATS_APPLICATION_ID + "-client";
        KAFKA_TOPIC_PATIENT_ALERTS = System.getenv("KAFKA_TOPIC_PATIENT_ALERTS");
        KAFKA_TOPIC_PATIENT_STATUS_COUNT = System.getenv("KAFKA_TOPIC_PATIENT_STATUS_COUNT");
    }
}
