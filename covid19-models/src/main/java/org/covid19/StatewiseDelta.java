package org.covid19;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@Data
public class StatewiseDelta {
    private Long deltaRecovered = 0L;
    private Long deltaDeaths = 0L;
    private Long deltaConfirmed = 0L;
    private Long currentRecovered = 0L;
    private Long currentDeaths = 0L;
    private Long currentConfirmed = 0L;
    private String lastUpdatedTime;
    private String state;
}
