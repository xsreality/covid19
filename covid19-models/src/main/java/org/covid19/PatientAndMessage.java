package org.covid19;

import com.google.gson.annotations.SerializedName;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@Data
public class PatientAndMessage {
    @SerializedName("message") private Message message;
    @SerializedName("patient_info") private PatientInfo patientInfo;
}
