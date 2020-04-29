package org.covid19.visualizations;

import org.apache.kafka.common.serialization.ByteArraySerializer;
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

@Configuration
public class ChartsProducer {
    private final KafkaProperties kafkaProperties;
    private final Serde<String> stringSerde;

    public ChartsProducer(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.stringSerde = Serdes.String();
    }

    @Bean
    public Map<String, Object> chartsProducerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerde.serializer().getClass().getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(CLIENT_ID_CONFIG, "org.covid19.patient-telegram-bot-charts-producer");
        return props;
    }

    @Bean
    public ProducerFactory<String, byte[]> chartsProducerFactory() {
        return new DefaultKafkaProducerFactory<>(chartsProducerConfigs());
    }

    @Bean
    public KafkaTemplate<String, byte[]> chartsKafkaTemplate() {
        return new KafkaTemplate<>(chartsProducerFactory());
    }
}
