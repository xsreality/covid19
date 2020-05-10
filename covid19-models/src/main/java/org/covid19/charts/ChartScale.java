package org.covid19.charts;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import static java.util.Collections.singletonList;

@ToString
@AllArgsConstructor
@Data
public class ChartScale {
    private List<ChartAxis> xAxes;
    private List<ChartAxis> yAxes;

    public ChartScale() {
        xAxes = singletonList(new ChartAxis("bottom-x-axis", "bottom", false));
        yAxes = singletonList(new ChartAxis("left-y-axis", "left", false));
    }

    public ChartScale(List<ChartAxis> yAxes) {
        xAxes = singletonList(new ChartAxis("bottom-x-axis", "bottom", false));
        this.yAxes = yAxes;
    }
}
