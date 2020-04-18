package org.covid19;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class UserPrefsSerde extends Serdes.WrapperSerde<UserPrefs> {
    public UserPrefsSerde() {
        super(new Serializer<UserPrefs>() {
            private Gson gson = new GsonBuilder().serializeNulls().create();

            @Override
            public byte[] serialize(String s, UserPrefs userPrefs) {
                return gson.toJson(userPrefs).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<UserPrefs>() {
            private Gson gson = new Gson();

            @Override
            public UserPrefs deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), UserPrefs.class);
            }
        });
    }
}
