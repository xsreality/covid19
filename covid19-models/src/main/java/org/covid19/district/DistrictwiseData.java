package org.covid19.district;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DistrictwiseData {
    private String state;
    private String district;
    private String confirmed;
    private String active;
    private String recovered;
    private String deceased;
    private String deltaConfirmed;
    private String deltaRecovered;
    private String deltaDeceased;
    private String notes;
}
