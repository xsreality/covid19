package org.covid19.charts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class PluginDatalabel {
    private Boolean display;
    private String anchor;
    private String backgroundColor;
    private String borderRadius;
    private String align;
}
