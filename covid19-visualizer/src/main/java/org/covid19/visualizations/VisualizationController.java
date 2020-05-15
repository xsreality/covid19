package org.covid19.visualizations;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.HttpStatus.OK;

@RestController
public class VisualizationController {

    private Visualizer visualizer;

    public VisualizationController(Visualizer visualizer) {
        this.visualizer = visualizer;
    }

    @ResponseStatus(code = OK)
    @GetMapping("/refresh")
    public void refresh() {
        try {
            visualizer.dailyAndTotalCharts();
            Thread.sleep(1000);
            visualizer.doublingRateChart();
            Thread.sleep(1000);
            visualizer.top5StatesTrend();
            Thread.sleep(1000);
            visualizer.testingTrend();
            Thread.sleep(1000);
            visualizer.historyTrend();
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
