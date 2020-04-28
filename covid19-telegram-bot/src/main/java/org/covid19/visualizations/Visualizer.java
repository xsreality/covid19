package org.covid19.visualizations;

import com.google.gson.Gson;

import org.covid19.StateStoresManager;
import org.covid19.StatewiseDelta;
import org.covid19.charts.Chart;
import org.covid19.charts.ChartData;
import org.covid19.charts.ChartDataset;
import org.covid19.charts.ChartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Long.parseLong;
import static java.time.ZoneId.of;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Objects.isNull;

@Component
public class Visualizer {
    private static final Logger LOG = LoggerFactory.getLogger(Visualizer.class);

    private static final String BLUE = "rgb(54, 162, 235)";
    private static final String RED = "rgb(255, 99, 132)";
    private static final String GREEN = "rgb(75, 192, 192)";

    private final StateStoresManager stateStores;
    private final VisualizationService visualizationService;
    private final KafkaTemplate<String, byte[]> chartsKafkaTemplate;

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(of("UTC"));
    private final DateTimeFormatter monthDayFormatter = DateTimeFormatter.ofPattern("MMM dd").withZone(of("UTC"));

    public static final String LAST_SEVEN_DAYS_OVERVIEW = "last7daysoverview";
    public static final String LAST_TWO_WEEKS_TOTAL = "last2weekstotal";
    public static final String DOUBLING_RATE = "doublingrate";

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

        final String dailyChartRequestJson = visualizationService.buildVisualizationRequest("bar", days, datasets);
        LOG.info("Request for Visualization service ready: {}", dailyChartRequestJson);
        byte[] dailyImage = visualizationService.buildVisualization(dailyChartRequestJson);

        LOG.info("Producing visualization to Kafka");
        chartsKafkaTemplate.send("visualizations", LAST_SEVEN_DAYS_OVERVIEW, dailyImage);

        datasets.clear();
        datasets.add(new ChartDataset("Confirmed", totalConfirmed, RED));
        datasets.add(new ChartDataset("Recovered", totalRecovered, GREEN));
        datasets.add(new ChartDataset("Deaths", totalDeceased, BLUE));

        final String totalChartRequestJson = visualizationService.buildVisualizationRequest("line", days, datasets);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", totalChartRequestJson);
        byte[] cumulativeImage = visualizationService.buildVisualization(totalChartRequestJson);
        chartsKafkaTemplate.send("visualizations", LAST_TWO_WEEKS_TOTAL, cumulativeImage);
    }

    @Scheduled(cron = "0 0 22 * * ?")
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

        ChartData chartData = new ChartData(days, datasets);
        Chart chart = new Chart("line", chartData);
        ChartRequest chartRequest = new ChartRequest(chart);

        final String doublingRateRequestJson = new Gson().toJson(chartRequest, ChartRequest.class);
        LOG.info("Request for 2 weeks cumulative chart ready: {}", doublingRateRequestJson);
        byte[] doublingRateImage = visualizationService.buildVisualization(doublingRateRequestJson);
        chartsKafkaTemplate.send("visualizations", DOUBLING_RATE, doublingRateImage);
    }
}
