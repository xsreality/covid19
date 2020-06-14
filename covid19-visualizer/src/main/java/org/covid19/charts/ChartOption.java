package org.covid19.charts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ChartOption {
    private ChartPlugin plugins;
    private ChartScale scales;
    private ChartTitle title;
}
