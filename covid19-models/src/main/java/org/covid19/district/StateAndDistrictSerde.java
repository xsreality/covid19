package org.covid19.district;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class StateAndDistrictSerde extends Serdes.WrapperSerde<StateAndDistrict> {
    public StateAndDistrictSerde() {
        super(new Serializer<StateAndDistrict>() {
            private Gson gson = new GsonBuilder().serializeNulls().disableInnerClassSerialization().create();

            @Override
            public byte[] serialize(String s, StateAndDistrict stateAndDistrict) {
                return gson.toJson(stateAndDistrict).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<StateAndDistrict>() {
            private Gson gson = new Gson();

            @Override
            public StateAndDistrict deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), StateAndDistrict.class);
            }
        });
    }
}
