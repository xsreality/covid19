package org.covid19;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class StateAndDateSerde extends Serdes.WrapperSerde<StateAndDate> {
    public StateAndDateSerde() {
        super(new Serializer<StateAndDate>() {
            private Gson gson = new GsonBuilder().serializeNulls().create();

            @Override
            public byte[] serialize(String s, StateAndDate stateAndDate) {
                return gson.toJson(stateAndDate).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<StateAndDate>() {
            private Gson gson = new Gson();

            @Override
            public StateAndDate deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), StateAndDate.class);
            }
        });
    }
}
