package org.covid19.charts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Chart {
    private String type;
    private ChartData data;
    private ChartOption options;

    public Chart(String type, ChartData data, boolean displayLabels) {
        this.type = type;
        this.data = data;
        PluginDatalabel datalabel = new PluginDatalabel(displayLabels, "end", "#ccc", "3", "end");
        this.options = new ChartOption(new ChartPlugin(datalabel));
    }
}
