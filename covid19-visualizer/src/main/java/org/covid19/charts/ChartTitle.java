package org.covid19.charts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@ToString
@AllArgsConstructor
@Data
public class ChartTitle {
    private boolean display;
    private String text;
}
