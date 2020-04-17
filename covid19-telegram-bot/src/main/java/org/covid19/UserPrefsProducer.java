package org.covid19;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Configuration
public class UserPrefsProducer {
    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;

    public UserPrefsProducer(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.stringSerde = Serdes.String();
    }

    @Bean
    public Map<String, Object> userPrefsProducerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());

        props.put(KEY_SERIALIZER_CLASS_CONFIG, stringSerde.serializer().getClass().getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
        props.put(CLIENT_ID_CONFIG, "org.covid19.patient-telegram-bot-user-prefs-producer");

        return props;
    }

    @Bean
    public ProducerFactory<String, UserPrefs> userPrefsProducerFactory() {
        return new DefaultKafkaProducerFactory<>(userPrefsProducerConfigs());
    }

    @Bean
    public KafkaTemplate<String, UserPrefs> userPrefsKafkaTemplate() {
        return new KafkaTemplate<>(userPrefsProducerFactory());
    }

}
