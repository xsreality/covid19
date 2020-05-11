package org.covid19.charts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ChartAxis {
    private String id;
    private String position;
    private Boolean stacked;
    private ChartTick ticks;

    public ChartAxis(String id, String position, Boolean stacked) {
        this.id = id;
        this.position = position;
        this.stacked = stacked;
        this.ticks = new ChartTick(true);
    }
}
