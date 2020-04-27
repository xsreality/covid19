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
    private String label;
    private List<Long> data;
    private Boolean fill;
    private String borderColor;
    private String backgroundColor;
    private String borderWidth;
    private String barThickness;

    public ChartDataset(String label, List<Long> data, String borderColor) {
        this.label = label;
        this.data = data;
        this.fill = false;
        this.borderColor = borderColor;
        this.backgroundColor = this.borderColor;
        this.borderWidth = "1";
        this.barThickness = "15";
    }
}
