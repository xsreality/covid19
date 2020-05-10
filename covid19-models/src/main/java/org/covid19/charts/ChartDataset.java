package org.covid19.charts;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ChartDataset {
    private String type;
    private String label;
    private List<Double> data;
    private Boolean fill;
    private String borderColor;
    private String backgroundColor;
    private String borderWidth;
    private String barThickness;
    private String pointRadius;
    private String yAxisID;

    public ChartDataset(String type, String label, List<Double> data, String borderColor) {
        this.type = type;
        this.label = label;
        this.data = data;
        this.fill = false;
        this.borderColor = borderColor;
        this.backgroundColor = this.borderColor;
        this.borderWidth = "3";
        this.barThickness = "10";
        this.pointRadius = "0";
        this.yAxisID = "left-y-axis";
    }

    public ChartDataset(String type, String label, List<Double> data, String borderColor, String yAxisID) {
        this.type = type;
        this.label = label;
        this.data = data;
        this.fill = false;
        this.borderColor = borderColor;
        this.backgroundColor = this.borderColor;
        this.borderWidth = "3";
        this.barThickness = "10";
        this.pointRadius = "0";
        this.yAxisID = yAxisID;
    }
}
