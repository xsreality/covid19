package org.covid19.visualizations;

import org.covid19.StateStoresManager;
import org.covid19.StatewiseDelta;
import org.covid19.charts.ChartDataset;
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

    public Visualizer(StateStoresManager stateStores, VisualizationService visualizationService,
                      KafkaTemplate<String, byte[]> chartsKafkaTemplate) {
        this.stateStores = stateStores;
        this.visualizationService = visualizationService;
        this.chartsKafkaTemplate = chartsKafkaTemplate;
    }

    @Scheduled(fixedDelay = 28800000, initialDelay = 60000)
    public void last7DaysOverview() {
        LOG.info("Generating visualization for last 7 days overview");
        Map<String, StatewiseDelta> data = new LinkedHashMap<>();
        for (long deltaDays = 7L; deltaDays >= 1L; deltaDays--) {
            String day = dateTimeFormatter.format(Instant.now().minus(deltaDays, DAYS));
            String monthDay = monthDayFormatter.format(Instant.now().minus(deltaDays, DAYS));
            StatewiseDelta count = stateStores.dailyCountFor("Total", day);
            data.put(monthDay, count);
        }

        List<String> days = new ArrayList<>();
        List<Long> confirmed = new ArrayList<>();
        List<Long> recovered = new ArrayList<>();
        List<Long> deceased = new ArrayList<>();
        List<ChartDataset> datasets = new ArrayList<>();

        data.forEach((day, delta) -> {
            if (isNull(delta)) {
                return;
            }
            days.add(day);
            confirmed.add(delta.getDeltaConfirmed());
            recovered.add(delta.getDeltaRecovered());
            deceased.add(delta.getDeltaDeaths());
            LOG.info("For day {}, count {}", day, delta);
        });

        datasets.add(new ChartDataset("Confirmed", confirmed, RED));
        datasets.add(new ChartDataset("Recovered", recovered, GREEN));
        datasets.add(new ChartDataset("Deaths", deceased, BLUE));

        final String chartRequestJson = visualizationService.buildVisualizationRequest(days, datasets);
        LOG.info("Request for Visualization service ready: {}", chartRequestJson);
        byte[] graphImage = visualizationService.buildVisualization(chartRequestJson);

        LOG.info("Producing visualization to Kafka");
        chartsKafkaTemplate.send("visualizations", LAST_SEVEN_DAYS_OVERVIEW, graphImage);
    }
}
