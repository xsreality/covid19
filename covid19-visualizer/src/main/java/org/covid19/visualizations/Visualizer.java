package org.covid19.visualizations;

import com.google.gson.Gson;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.covid19.StateAndDate;
import org.covid19.StateStoresManager;
import org.covid19.StatewiseDelta;
import org.covid19.StatewiseTestData;
import org.covid19.charts.Chart;
import org.covid19.charts.ChartAxis;
import org.covid19.charts.ChartData;
import org.covid19.charts.ChartDataset;
import org.covid19.charts.ChartRequest;
import org.covid19.charts.ChartTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static java.lang.Double.valueOf;
import static java.time.ZoneId.of;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

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

    private static final String LINE = "line";
    private static final String BAR = "bar";

    private final StateStoresManager stateStores;
    private final VisualizationService visualizationService;
    private final KafkaTemplate<String, byte[]> chartsKafkaTemplate;

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
    private final DateTimeFormatter monthDayFormatter = DateTimeFormatter.ofPattern("MMM dd").withZone(of("UTC"));
    private final DecimalFormat df = new DecimalFormat("0.00");

    public static final String LAST_SEVEN_DAYS_OVERVIEW = "last7daysoverview";
    public static final String LAST_TWO_WEEKS_TOTAL = "last2weekstotal";
    public static final String DOUBLING_RATE = "doublingrate";
    public static final String STATES_TREND = "top5statestrend";
    public static final String HISTORY_TREND = "historytrend";
    public static final String TESTING_TREND = "testingtotal";

    public Visualizer(StateStoresManager stateStores, VisualizationService visualizationService,
                      KafkaTemplate<String, byte[]> chartsKafkaTemplate) {
        this.stateStores = stateStores;
        this.visualizationService = visualizationService;
        this.chartsKafkaTemplate = chartsKafkaTemplate;
    }

    @Scheduled(cron = "0 0 02 * * ?")
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
        List<Double> dailyConfirmed = new ArrayList<>();
        List<Double> dailyRecovered = new ArrayList<>();
        List<Double> dailyDeceased = new ArrayList<>();
        List<Double> totalConfirmed = new ArrayList<>();
        List<Double> totalRecovered = new ArrayList<>();
        List<Double> totalDeceased = new ArrayList<>();
        List<ChartDataset> datasets = new ArrayList<>();

        data.forEach((day, delta) -> {
            if (isNull(delta)) {
                return;
            }
            days.add(day);
            dailyConfirmed.add(valueOf(delta.getDeltaConfirmed()));
            dailyRecovered.add(valueOf(delta.getDeltaRecovered()));
            dailyDeceased.add(valueOf(delta.getDeltaDeaths()));
            totalConfirmed.add(valueOf(delta.getCurrentConfirmed()));
            totalRecovered.add(valueOf(delta.getCurrentRecovered()));
            totalDeceased.add(valueOf(delta.getCurrentDeaths()));
            LOG.info("For day {}, count {}", day, delta);
        });

        datasets.add(new ChartDataset(BAR, "Confirmed", dailyConfirmed, RED));
        datasets.add(new ChartDataset(BAR, "Recovered", dailyRecovered, GREEN));
        datasets.add(new ChartDataset(BAR, "Deaths", dailyDeceased, BLUE));

        final String dailyChartRequestJson = visualizationService.buildVisualizationRequest(BAR, days, datasets, true);
        LOG.info("Request for Visualization service ready: {}", dailyChartRequestJson);
        byte[] dailyImage = visualizationService.buildVisualization(dailyChartRequestJson);

        LOG.info("Producing visualization to Kafka");
        chartsKafkaTemplate.send("visualizations", LAST_SEVEN_DAYS_OVERVIEW, dailyImage);

        datasets.clear();
        datasets.add(new ChartDataset(LINE, "Confirmed", totalConfirmed, RED));
        datasets.add(new ChartDataset(LINE, "Recovered", totalRecovered, GREEN));
        datasets.add(new ChartDataset(LINE, "Deaths", totalDeceased, BLUE));

        final String totalChartRequestJson = visualizationService.buildVisualizationRequest(LINE, days, datasets, true);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", totalChartRequestJson);
        byte[] cumulativeImage = visualizationService.buildVisualization(totalChartRequestJson);
        chartsKafkaTemplate.send("visualizations", LAST_TWO_WEEKS_TOTAL, cumulativeImage);
    }

    @Scheduled(cron = "0 2 02 * * ?")
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
        List<Double> doublingRate = new ArrayList<>();
        List<ChartDataset> datasets = new ArrayList<>();

        data.forEach((day, rate) -> {
            if (isNull(rate)) {
                LOG.error("found null value for {}", day);
                return;
            }
            days.add(day);
            doublingRate.add(parseDouble(rate));
            LOG.info("For day {}, rate {}", day, rate);
        });

        datasets.add(new ChartDataset(LINE, "Doubling Rate", doublingRate, RED));

        final String doublingRateRequestJson = visualizationService.buildVisualizationRequest(LINE, days, datasets, true);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", doublingRateRequestJson);
        byte[] doublingRateImage = visualizationService.buildVisualization(doublingRateRequestJson);
        chartsKafkaTemplate.send("visualizations", DOUBLING_RATE, doublingRateImage);
    }

    @Scheduled(cron = "0 3 02 * * ?")
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
        Map<String, List<Double>> stateCasesByDate = new LinkedHashMap<>();
        data.forEach((day, stateCasesData) -> {
            stateCasesData.forEach((state, total) -> {
                stateCasesByDate.computeIfAbsent(state, s -> new ArrayList<>()).add(valueOf(total));
            });
        });

        // create datasets
        List<ChartDataset> datasets = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, List<Double>> entry : stateCasesByDate.entrySet()) {
            String state = entry.getKey();
            List<Double> cases = entry.getValue();
            datasets.add(new ChartDataset(LINE, state, cases, colors.get(i++)));
        }

        final String statesTrendRequestJson = visualizationService.buildVisualizationRequest(LINE, new ArrayList<>(data.keySet()), datasets, false);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", statesTrendRequestJson);
        byte[] statesTrendImage = visualizationService.buildVisualization(statesTrendRequestJson);
        chartsKafkaTemplate.send("visualizations", STATES_TREND, statesTrendImage);
    }

    @Scheduled(cron = "0 4 02 * * ?")
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
        List<Double> totalCases = new ArrayList<>();
        List<Double> recovered = new ArrayList<>();
        List<Double> deceased = new ArrayList<>();
        data.forEach((day, delta) -> {
            if (isNull(delta)) {
                LOG.info("Found null delta for {}", day);
                return;
            }
            totalCases.add(valueOf(delta.getCurrentConfirmed()));
            recovered.add(valueOf(delta.getCurrentRecovered()));
            deceased.add(valueOf(delta.getCurrentDeaths()));
            days.add(day);
        });

        // create datasets
        List<ChartDataset> datasets = new ArrayList<>(asList(
                new ChartDataset(LINE, "Total Cases", totalCases, BLUE),
                new ChartDataset(LINE, "Recovered", recovered, GREEN),
                new ChartDataset(LINE, "Deceased", deceased, RED)));

        final String historyTrendRequestJson = visualizationService.buildVisualizationRequest(LINE, days, datasets, false);
        LOG.info("Request for history trend chart ready: {}", historyTrendRequestJson);
        byte[] historyTrendImage = visualizationService.buildVisualization(historyTrendRequestJson);
        chartsKafkaTemplate.send("visualizations", HISTORY_TREND, historyTrendImage);
    }

    @Scheduled(cron = "0 5 02 * * ?")
    public void testingTrend() {
        LOG.info("Generating testing chart");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
        DateTimeFormatter monthDayFormatter = DateTimeFormatter.ofPattern("MMM dd").withZone(of("UTC"));

        final LocalDate startDate = dateTimeFormatter.parse("12/04/2020", LocalDate::from);// data available from here
        final LocalDate yesterday = LocalDate.now().minus(1L, DAYS);
        Map<String, StatewiseTestData> dailyTestedData = new LinkedHashMap<>();
        Map<String, StatewiseDelta> dailyPositiveData = new LinkedHashMap<>();
        Map<String, Double> fiveDayMovingAvgData = new LinkedHashMap<>();

        DescriptiveStatistics subset = new DescriptiveStatistics(5);

        LocalDate date = startDate;
        while (date.isBefore(yesterday)) {
            String fDate = dateTimeFormatter.format(date);
            String monthDay = monthDayFormatter.format(date);
            final StatewiseTestData tested = stateStores.testDataFor("Total", fDate);
            final StatewiseDelta positive = stateStores.dailyCountFor("Total", fDate);
            dailyTestedData.put(monthDay, tested);
            dailyPositiveData.put(monthDay, positive);

            if (nonNull(positive) && nonNull(tested)) {
                subset.addValue(valueOf(positive.getDeltaConfirmed()) / parseDouble(tested.getTestReportedToday()) * 100.0);
                fiveDayMovingAvgData.put(monthDay, subset.getMean());
            } else {
                fiveDayMovingAvgData.put(monthDay, subset.getMean());
            }

            date = date.plus(1L, DAYS);
        }

        List<String> days = new ArrayList<>();
        List<Double> dailyTested = new ArrayList<>();
        List<Double> dailyPositive = new ArrayList<>();
        List<Double> positivityRate = new ArrayList<>();
        dailyTestedData.forEach((day, tested) -> {
            if (isNull(tested)) {
                dailyTested.add(0.0);
                return;
            }
            dailyTested.add(parseDouble(tested.getTestReportedToday()));
            days.add(day);
        });
        dailyPositiveData.forEach((day, positive) -> {
            if ((isNull(positive))) {
                dailyPositive.add(0.0);
                return;
            }
            dailyPositive.add(valueOf(positive.getDeltaConfirmed()));
        });
        fiveDayMovingAvgData.forEach((day, rate) -> {
            if (isNull(rate) || Double.isNaN(rate)) {
                positivityRate.add(0.0);
                return;
            }
            LOG.info("Day: {}. Rate: {}", day, rate);
            positivityRate.add(valueOf(df.format(rate)));
        });

        ChartData chartData = new ChartData(
                new ArrayList<>(days),
                asList(new ChartDataset("bar", "Positive", dailyPositive, RED, "left-y-axis"),
                        new ChartDataset("bar", "Tested", dailyTested, GREEN, "left-y-axis"),
                        new ChartDataset("line", "5-day Moving Positivity rate", positivityRate, BLUE, "right-y-axis")));

        List<ChartAxis> xAxes = singletonList(new ChartAxis("bottom-x-axis", "bottom", true));
        List<ChartAxis> yAxes = asList(
                new ChartAxis("left-y-axis", "left", true),
                new ChartAxis("right-y-axis", "right", false, new ChartTick(true)));

        Chart chart = new Chart("bar", chartData, false, xAxes, yAxes);
        ChartRequest chartRequest = new ChartRequest(chart);

        final String testingRequestJson = new Gson().toJson(chartRequest, ChartRequest.class);
        LOG.info("Request for Testing chart ready: {}", testingRequestJson);
        byte[] testingImage = visualizationService.buildVisualization(testingRequestJson);
        chartsKafkaTemplate.send("visualizations", TESTING_TREND, testingImage);

    }
}
