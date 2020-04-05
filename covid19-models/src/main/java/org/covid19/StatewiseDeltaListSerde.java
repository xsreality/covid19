package org.covid19;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class StatewiseDeltaListSerde extends Serdes.WrapperSerde<ArrayList<StatewiseDelta>> {
    public StatewiseDeltaListSerde() {
        super(new Serializer<ArrayList<StatewiseDelta>>() {
            private Gson gson = new GsonBuilder().serializeNulls().create();

            @Override
            public byte[] serialize(String s, ArrayList<StatewiseDelta> statewiseDeltas) {
                return gson.toJson(statewiseDeltas).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<ArrayList<StatewiseDelta>>() {
            private Gson gson = new Gson();

            @Override
            public ArrayList<StatewiseDelta> deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), new TypeToken<ArrayList<StatewiseDelta>>(){}.getType());
            }
        });
    }
}
