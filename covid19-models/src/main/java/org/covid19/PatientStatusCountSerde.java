package org.covid19;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class PatientStatusCountSerde extends Serdes.WrapperSerde<PatientStatusCount> {
    public PatientStatusCountSerde() {
        super(new Serializer<PatientStatusCount>() {
            private Gson gson = new GsonBuilder().serializeNulls().create();

            @Override
            public byte[] serialize(String s, PatientStatusCount patientStatusCount) {
                return gson.toJson(patientStatusCount).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<PatientStatusCount>() {
            private Gson gson = new Gson();

            @Override
            public PatientStatusCount deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), PatientStatusCount.class);
            }
        });
    }
}
