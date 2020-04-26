package org.covid19;

import com.google.gson.annotations.SerializedName;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class StatewiseTestData {
    private String negative;
    @SerializedName("numcallsstatehelpline") private String numCallsStateHelpline;
    @SerializedName("numicubeds") private String numIcuBeds;
    @SerializedName("numisolationbeds") private String numIsolationBeds;
    @SerializedName("numventilators") private String numVentilators;
    private String positive;
    private String source;
    private String source2;
    private String state;
    @SerializedName("testpositivityrate") private String testPositivityRate;
    @SerializedName("testsperthousand") private String testsPerThousand;
    @SerializedName("totalpeopleinquarantine") private String tootalPeopleInQuarantine;
    @SerializedName("totalpeoplereleasedfromquarantine") private String tootalPeopleReleasedFromQuarantine;
    @SerializedName("totaltested") private String totalTested;
    private String unconfirmed;
    private String updatedon;
    @SerializedName("testreportedtoday") private String testReportedToday;
    @SerializedName("positivereportedtoday") private String positiveReportedToday;
}
