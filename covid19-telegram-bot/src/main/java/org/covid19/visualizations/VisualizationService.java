package org.covid19.visualizations;

import com.google.gson.Gson;

import org.covid19.charts.Chart;
import org.covid19.charts.ChartData;
import org.covid19.charts.ChartDataset;
import org.covid19.charts.ChartRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON;

@Component
public class VisualizationService {

    @Value("${visualization.url}")
    private String visualizationUrl;

    private final RestTemplate restTemplate;

    public VisualizationService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String buildVisualizationRequest(String type, List<String> labels, List<ChartDataset> datasets) {
        ChartData chartData = new ChartData(labels, datasets);
        Chart chart = new Chart(type, chartData);
        ChartRequest chartRequest = new ChartRequest(chart);
        return new Gson().toJson(chartRequest, ChartRequest.class);
    }

    public byte[] buildVisualization(String request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(request, headers);
        return restTemplate.postForObject(visualizationUrl, entity, byte[].class);
    }
}
