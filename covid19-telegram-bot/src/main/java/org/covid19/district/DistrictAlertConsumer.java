package org.covid19.district;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.covid19.StateStoresManager;
import org.covid19.StatewiseDelta;
import org.covid19.bot.Covid19Bot;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import static io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_KEY_TYPE;
import static io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;
import static java.lang.Long.parseLong;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST;
import static org.covid19.bot.BotUtils.sendTelegramAlert;
import static org.covid19.district.DistrictAlertUtils.buildDistrictwiseAlert;

@Configuration
@Slf4j
public class DistrictAlertConsumer {
    private final KafkaProperties kafkaProperties;
    private final Covid19Bot covid19Bot;
    private final StateStoresManager stateStores;

    @Value("${telegram.creator.id}")
    private String telegramCreatorId;

    public DistrictAlertConsumer(KafkaProperties kafkaProperties, Covid19Bot covid19Bot, StateStoresManager stateStores) {
        this.kafkaProperties = kafkaProperties;
        this.covid19Bot = covid19Bot;
        this.stateStores = stateStores;
    }

    @Bean
    public Map<String, Object> districtAlertsConsumerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, LATEST);
        props.put(GROUP_ID_CONFIG, "org.covid19.telegram-bot-district-alerts-consumer");
        props.put(CLIENT_ID_CONFIG, "org.covid19.telegram-bot-district-alerts-consumer-client");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(JSON_KEY_TYPE, StateAndDistrict.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(JSON_VALUE_TYPE, DistrictwiseData.class);
        return props;
    }

    @Bean
    public ConsumerFactory<String, StatewiseDelta> districtAlertsConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(districtAlertsConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StatewiseDelta> districtAlertsKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StatewiseDelta> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(districtAlertsConsumerFactory());
        factory.setMissingTopicsFatal(false);
        factory.setBatchListener(true);
        return factory;
    }

    @KafkaListener(topics = "districtwise-delta", id = "districtwiseAlertsConsumer",
            idIsGroup = false, autoStartup = "false",
            containerFactory = "districtAlertsKafkaListenerContainerFactory")
    public void listenDistrictAlerts(@Payload List<DistrictwiseData> deltas) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignore
        }

        List<DistrictwiseData> readyToSend = new ArrayList<>();
        List<DistrictwiseData> dailyIncrements = new ArrayList<>();

        for (DistrictwiseData delta : deltas) {
            if (parseLong(delta.getDeltaRecovered()) < 1L
                    && parseLong(delta.getDeltaConfirmed()) < 1L
                    && parseLong(delta.getDeltaDeceased()) < 1L) {
                continue;
            }
            readyToSend.add(delta);
            dailyIncrements.add(stateStores.dailyStatsForStateAndDistrict(delta.getState(), delta.getDistrict()));
        }
        if (readyToSend.isEmpty()) {
            return; // no useful update
        }

        String alertText = buildDistrictwiseAlert(readyToSend, dailyIncrements);
        sendTelegramAlert(covid19Bot, telegramCreatorId, alertText, null, true);
    }
}
