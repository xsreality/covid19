package org.covid19;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class StatewiseDeltaSerde extends Serdes.WrapperSerde<StatewiseDelta> {
    public StatewiseDeltaSerde() {
        super(new Serializer<StatewiseDelta>() {
            private Gson gson = new GsonBuilder().serializeNulls().create();

            @Override
            public byte[] serialize(String s, StatewiseDelta statewiseDelta) {
                return gson.toJson(statewiseDelta).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<StatewiseDelta>() {
            private Gson gson = new Gson();

            @Override
            public StatewiseDelta deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), StatewiseDelta.class);
            }
        });
    }
}
