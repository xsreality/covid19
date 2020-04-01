package org.covid19;

import com.google.gson.Gson;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class TweetSerde extends Serdes.WrapperSerde<Tweet> {

    public TweetSerde() {
        super(new Serializer<Tweet>() {
            private Gson gson = new Gson();

            @Override
            public byte[] serialize(String s, Tweet tweet) {
                return gson.toJson(tweet).getBytes(StandardCharsets.UTF_8);
            }
        }, new Deserializer<Tweet>() {
            private Gson gson = new Gson();

            @Override
            public Tweet deserialize(String s, byte[] bytes) {
                return gson.fromJson(new String(bytes), Tweet.class);
            }
        });
    }
}
