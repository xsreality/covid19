package org.covid19;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class StatewiseTestDataSerde extends Serdes.WrapperSerde<StatewiseTestData> {
    public StatewiseTestDataSerde() {
        super(new Serializer<StatewiseTestData>() {
            private Gson gson = new GsonBuilder().serializeNulls().disableInnerClassSerialization().create();

            @Override
            public byte[] serialize(String s, StatewiseTestData statewiseTestData) {
                return gson.toJson(statewiseTestData).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<StatewiseTestData>() {
            private Gson gson = new Gson();

            @Override
            public StatewiseTestData deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), StatewiseTestData.class);
            }
        });
    }
}
