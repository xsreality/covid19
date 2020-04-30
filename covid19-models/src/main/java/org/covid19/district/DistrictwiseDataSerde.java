package org.covid19.district;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class DistrictwiseDataSerde extends Serdes.WrapperSerde<DistrictwiseData> {
    public DistrictwiseDataSerde() {
        super(new Serializer<DistrictwiseData>() {
            private Gson gson = new GsonBuilder().serializeNulls().disableInnerClassSerialization().create();

            @Override
            public byte[] serialize(String s, DistrictwiseData districtwiseData) {
                return gson.toJson(districtwiseData).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<DistrictwiseData>() {
            private Gson gson = new Gson();

            @Override
            public DistrictwiseData deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), DistrictwiseData.class);
            }
        });
    }
}
