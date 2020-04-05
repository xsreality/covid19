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
public class StatewiseStats {
    private String active;
    private String confirmed;
    private String deaths;
    private String recovered;
    private String state;
    @SerializedName("statecode") private String stateCode;
    @SerializedName("lastupdatedtime") private String lastUpdatedTime;
}
