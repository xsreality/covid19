package org.covid19;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class StatewiseStatsSerde extends Serdes.WrapperSerde<StatewiseStats> {
    public StatewiseStatsSerde() {
        super(new Serializer<StatewiseStats>() {
            private Gson gson = new GsonBuilder().serializeNulls().disableInnerClassSerialization().create();

            @Override
            public byte[] serialize(String s, StatewiseStats StatewiseStats) {
                return gson.toJson(StatewiseStats).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<StatewiseStats>() {
            private Gson gson = new Gson();

            @Override
            public StatewiseStats deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), StatewiseStats.class);
            }
        });
    }
}
