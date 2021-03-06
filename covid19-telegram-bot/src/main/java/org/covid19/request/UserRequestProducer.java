package org.covid19.request;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.covid19.UserRequest;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class UserRequestProducer {
    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;

    public UserRequestProducer(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.stringSerde = Serdes.String();
    }

    @Bean
    public Map<String, Object> userRequestProducerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());

        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerde.serializer().getClass().getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");

        return props;
    }

    @Bean
    public ProducerFactory<String, UserRequest> userRequestProducerFactory() {
        return new DefaultKafkaProducerFactory<>(userRequestProducerConfigs());
    }

    @Bean
    public KafkaTemplate<String, UserRequest> userRequestKafkaTemplate() {
        return new KafkaTemplate<>(userRequestProducerFactory());
    }
}
