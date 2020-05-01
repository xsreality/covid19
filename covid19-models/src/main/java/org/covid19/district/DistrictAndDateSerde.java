package org.covid19.district;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class DistrictAndDateSerde extends Serdes.WrapperSerde<DistrictAndDate> {
    public DistrictAndDateSerde() {
        super(new Serializer<DistrictAndDate>() {
            private Gson gson = new GsonBuilder().serializeNulls().disableInnerClassSerialization().create();

            @Override
            public byte[] serialize(String s, DistrictAndDate districtAndDate) {
                return gson.toJson(districtAndDate).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<DistrictAndDate>() {
            private Gson gson = new Gson();

            @Override
            public DistrictAndDate deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), DistrictAndDate.class);
            }
        });
    }
}
