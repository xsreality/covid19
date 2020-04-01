package org.covid19;

import com.google.gson.annotations.SerializedName;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class PatientInfo {
    @SerializedName("_d180g") private String d180G;
    @SerializedName("agebracket") private String ageBracket;
    @SerializedName("backupnotes") private String backupNotes;
    @SerializedName("notes") private String notes;
    @SerializedName("contractedfromwhichpatientsuspected") private String contractedFromWhichPatientSuspected;
    @SerializedName("currentstatus") private String currentStatus;
    @SerializedName("dateannounced") private String dateAnnounced;
    @SerializedName("detectedcity") private String detectedCity;
    @SerializedName("detecteddistrict") private String detectedDistrict;
    @SerializedName("detectedstate") private String detectedState;
    @SerializedName("estimatedonsetdate") private String estimatedOnsetDate;
    @SerializedName("gender") private String gender;
    @SerializedName("nationality") private String nationality;
    @SerializedName("patientnumber") private String patientNumber;
    @SerializedName("source1") private String source1;
    @SerializedName("source2") private String source2;
    @SerializedName("source3") private String source3;
    @SerializedName("statepatientnumber") private String statePatientNumber;
    @SerializedName("statuschangedate") private String statusChangeDate;
}
