package org.covid19.visualizations;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.covid19.StateAndDate;
import org.covid19.StateStoresManager;
import org.covid19.StatewiseDelta;
import org.covid19.charts.ChartDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Long.parseLong;
import static java.time.ZoneId.of;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static java.util.Objects.isNull;

@Component
public class Visualizer {
    private static final Logger LOG = LoggerFactory.getLogger(Visualizer.class);

    private static final String BLUE = "rgb(54, 162, 235)";
    private static final String RED = "rgb(255, 99, 132)";
    private static final String GREEN = "rgb(75, 192, 192)";
    private static final String ORANGE = "rgb(255, 159, 64)";
    private static final String YELLOW = "rgb(255, 205, 86)";
    private static final String PURPLE = "rgb(153, 102, 255)";
    private static final String GREY = "rgb(201, 203, 207)";

    private final StateStoresManager stateStores;
    private final VisualizationService visualizationService;
    private final KafkaTemplate<String, byte[]> chartsKafkaTemplate;

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
    private final DateTimeFormatter monthDayFormatter = DateTimeFormatter.ofPattern("MMM dd").withZone(of("UTC"));

    public static final String LAST_SEVEN_DAYS_OVERVIEW = "last7daysoverview";
    public static final String LAST_TWO_WEEKS_TOTAL = "last2weekstotal";
    public static final String DOUBLING_RATE = "doublingrate";
    public static final String STATES_TREND = "top5statestrend";
    public static final String HISTORY_TREND = "historytrend";

    public Visualizer(StateStoresManager stateStores, VisualizationService visualizationService,
                      KafkaTemplate<String, byte[]> chartsKafkaTemplate) {
        this.stateStores = stateStores;
        this.visualizationService = visualizationService;
        this.chartsKafkaTemplate = chartsKafkaTemplate;
    }

    @Scheduled(cron = "0 0 22 * * ?")
    public void dailyAndTotalCharts() {
        LOG.info("Generating visualization for last 7 days overview");
        Map<String, StatewiseDelta> data = new LinkedHashMap<>();
        for (long deltaDays = 14L; deltaDays >= 1L; deltaDays--) {
            String day = dateTimeFormatter.format(Instant.now().minus(deltaDays, DAYS));
            String monthDay = monthDayFormatter.format(Instant.now().minus(deltaDays, DAYS));
            StatewiseDelta count = stateStores.dailyCountFor("Total", day);
            data.put(monthDay, count);
        }

        List<String> days = new ArrayList<>();
        List<Long> dailyConfirmed = new ArrayList<>();
        List<Long> dailyRecovered = new ArrayList<>();
        List<Long> dailyDeceased = new ArrayList<>();
        List<Long> totalConfirmed = new ArrayList<>();
        List<Long> totalRecovered = new ArrayList<>();
        List<Long> totalDeceased = new ArrayList<>();
        List<ChartDataset> datasets = new ArrayList<>();

        data.forEach((day, delta) -> {
            if (isNull(delta)) {
                return;
            }
            days.add(day);
            dailyConfirmed.add(delta.getDeltaConfirmed());
            dailyRecovered.add(delta.getDeltaRecovered());
            dailyDeceased.add(delta.getDeltaDeaths());
            totalConfirmed.add(delta.getCurrentConfirmed());
            totalRecovered.add(delta.getCurrentRecovered());
            totalDeceased.add(delta.getCurrentDeaths());
            LOG.info("For day {}, count {}", day, delta);
        });

        datasets.add(new ChartDataset("Confirmed", dailyConfirmed, RED));
        datasets.add(new ChartDataset("Recovered", dailyRecovered, GREEN));
        datasets.add(new ChartDataset("Deaths", dailyDeceased, BLUE));

        final String dailyChartRequestJson = visualizationService.buildVisualizationRequest("bar", days, datasets, true);
        LOG.info("Request for Visualization service ready: {}", dailyChartRequestJson);
        byte[] dailyImage = visualizationService.buildVisualization(dailyChartRequestJson);

        LOG.info("Producing visualization to Kafka");
        chartsKafkaTemplate.send("visualizations", LAST_SEVEN_DAYS_OVERVIEW, dailyImage);

        datasets.clear();
        datasets.add(new ChartDataset("Confirmed", totalConfirmed, RED));
        datasets.add(new ChartDataset("Recovered", totalRecovered, GREEN));
        datasets.add(new ChartDataset("Deaths", totalDeceased, BLUE));

        final String totalChartRequestJson = visualizationService.buildVisualizationRequest("line", days, datasets, true);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", totalChartRequestJson);
        byte[] cumulativeImage = visualizationService.buildVisualization(totalChartRequestJson);
        chartsKafkaTemplate.send("visualizations", LAST_TWO_WEEKS_TOTAL, cumulativeImage);
    }

    @Scheduled(cron = "0 2 22 * * ?")
    public void doublingRateChart() {
        LOG.info("Generating doubling rate chart");
        Map<String, String> data = new LinkedHashMap<>();
        for (long deltaDays = 31L; deltaDays >= 1L; deltaDays--) {
            String day = dateTimeFormatter.format(Instant.now().minus(deltaDays, DAYS));
            String monthDay = monthDayFormatter.format(Instant.now().minus(deltaDays, DAYS));
            String count = stateStores.doublingRateFor("Total", day);
            data.put(monthDay, count);
        }

        List<String> days = new ArrayList<>();
        List<Long> doublingRate = new ArrayList<>();
        List<ChartDataset> datasets = new ArrayList<>();

        data.forEach((day, rate) -> {
            if (isNull(rate)) {
                LOG.error("found null value for {}", day);
                return;
            }
            days.add(day);
            doublingRate.add(parseLong(rate));
            LOG.info("For day {}, rate {}", day, rate);
        });

        datasets.add(new ChartDataset("Doubling Rate", doublingRate, RED));

        final String doublingRateRequestJson = visualizationService.buildVisualizationRequest("line", days, datasets, true);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", doublingRateRequestJson);
        byte[] doublingRateImage = visualizationService.buildVisualization(doublingRateRequestJson);
        chartsKafkaTemplate.send("visualizations", DOUBLING_RATE, doublingRateImage);
    }

    @Scheduled(cron = "0 3 22 * * ?")
    public void top5StatesTrend() {
        LOG.info("Generating top 5 states trend chart");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
        DateTimeFormatter monthDayFormatter = DateTimeFormatter.ofPattern("MMM dd").withZone(of("UTC"));

        List<String> interestingStates = asList("Maharashtra", "Gujarat", "Delhi", "Madhya Pradesh", "Rajasthan");
        List<String> colors = asList(RED, YELLOW, GREEN, BLUE, ORANGE, PURPLE);

        Map<String, Map<String, Long>> data = new LinkedHashMap<>();
        for (long deltaDays = 14L; deltaDays >= 1L; deltaDays--) {
            String day = dateTimeFormatter.format(Instant.now().minus(deltaDays, DAYS));
            String monthDay = monthDayFormatter.format(Instant.now().minus(deltaDays, DAYS));
            final KeyValueIterator<StateAndDate, StatewiseDelta> all = stateStores.dailyCount();
            Map<String, Long> statesData = new LinkedHashMap<>();
            while (all.hasNext()) {
                final KeyValue<StateAndDate, StatewiseDelta> next = all.next();
                StateAndDate stateAndDate = next.key;
                StatewiseDelta statewiseDelta = next.value;
                if (!day.equalsIgnoreCase(stateAndDate.getDate())) {
                    continue;
                }
                if (!interestingStates.contains(stateAndDate.getState())) {
                    continue;
                }
                statesData.putIfAbsent(stateAndDate.getState(), statewiseDelta.getCurrentConfirmed());
            }
            data.put(monthDay, statesData);
        }

        // aggregate by state -> [case numbers]
        Map<String, List<Long>> stateCasesByDate = new LinkedHashMap<>();
        data.forEach((day, stateCasesData) -> {
            stateCasesData.forEach((state, total) -> {
                stateCasesByDate.computeIfAbsent(state, s -> new ArrayList<>()).add(total);
            });
        });

        // create datasets
        List<ChartDataset> datasets = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, List<Long>> entry : stateCasesByDate.entrySet()) {
            String state = entry.getKey();
            List<Long> cases = entry.getValue();
            datasets.add(new ChartDataset(state, cases, colors.get(i++)));
        }

        final String statesTrendRequestJson = visualizationService.buildVisualizationRequest("line", new ArrayList<>(data.keySet()), datasets, false);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", statesTrendRequestJson);
        byte[] statesTrendImage = visualizationService.buildVisualization(statesTrendRequestJson);
        chartsKafkaTemplate.send("visualizations", STATES_TREND, statesTrendImage);
    }

    @Scheduled(cron = "0 4 22 * * ?")
    public void historyTrend() {
        LOG.info("Generating history trend chart");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
        DateTimeFormatter monthDayFormatter = DateTimeFormatter.ofPattern("MMM dd").withZone(of("UTC"));

        final LocalDate startDate = dateTimeFormatter.parse("30/01/2020", LocalDate::from);// data available from here
        final LocalDate yesterday = LocalDate.now().minus(1L, DAYS);
        Map<String, StatewiseDelta> data = new LinkedHashMap<>();

        LocalDate date = startDate;
        while (date.isBefore(yesterday)) {
            String fDate = dateTimeFormatter.format(date);
            String monthDay = monthDayFormatter.format(date);
            data.put(monthDay, stateStores.dailyCountFor("Total", fDate));
            date = date.plus(1L, DAYS);
        }

        List<String> days = new ArrayList<>();
        List<Long> totalCases = new ArrayList<>();
        List<Long> recovered = new ArrayList<>();
        List<Long> deceased = new ArrayList<>();
        data.forEach((day, delta) -> {
            if (isNull(delta)) {
                return;
            }
            totalCases.add(delta.getCurrentConfirmed());
            recovered.add(delta.getCurrentRecovered());
            deceased.add(delta.getCurrentDeaths());
            days.add(day);
        });

        // create datasets
        List<ChartDataset> datasets = new ArrayList<>(asList(
                new ChartDataset("Total Cases", totalCases, BLUE),
                new ChartDataset("Recovered", recovered, GREEN),
                new ChartDataset("Deceased", deceased, RED)));

        final String historyTrendRequestJson = visualizationService.buildVisualizationRequest("line", days, datasets, false);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", historyTrendRequestJson);
        byte[] historyTrendImage = visualizationService.buildVisualization(historyTrendRequestJson);
        chartsKafkaTemplate.send("visualizations", HISTORY_TREND, historyTrendImage);
    }
}
