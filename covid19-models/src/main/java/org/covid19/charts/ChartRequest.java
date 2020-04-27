package org.covid19.charts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ChartRequest {
    private String backgroundColor;
    private String width;
    private String height;
    private String format;
    private Chart chart;

    public ChartRequest(Chart chart) {
        this.backgroundColor = "transparent";
        this.width = "500";
        this.height = "300";
        this.format = "png";
        this.chart = chart;
    }
}
