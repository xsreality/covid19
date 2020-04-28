package org.covid19;

import org.covid19.bot.Covid19Bot;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import org.telegram.abilitybots.api.db.MapDBContext;
import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.io.File;
import java.util.List;

@SpringBootApplication
public class TelegramApp implements CommandLineRunner {

    @Value("${telegram.db.path}")
    private String telegramDbPath;

    @Value("${telegram.bot.username}")
    private String telegramBotUsername;

    @Value("${telegram.bot.token}")
    private String telegramBotToken;

    @Value("${telegram.creator.id}")
    private String telegramCreatorId;

    @Value("${telegram.chat.id}")
    private String telegramChatId;

    @Override
    public void run(String... args) {
        try {
            ApiContextInitializer.init();
            TelegramBotsApi botsApi = new TelegramBotsApi();
            botsApi.registerBot(covid19Bot());
        } catch (TelegramApiException e) {
            throw new IllegalStateException("Unable to register Telegram bot", e);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(TelegramApp.class, args);
    }

    @Bean
    public Covid19Bot covid19Bot() {
        DB db = DBMaker
                .fileDB(new File(telegramDbPath))
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .transactionEnable()
                .make();

        return new Covid19Bot(telegramBotToken, telegramBotUsername, new MapDBContext(db), telegramCreatorId, telegramChatId);
    }

    @Bean
    public RestTemplate restTemplate(List<HttpMessageConverter<?>> messageConverters) {
        return new RestTemplate(messageConverters);
    }

    @Bean
    public ByteArrayHttpMessageConverter byteArrayHttpMessageConverter() {
        return new ByteArrayHttpMessageConverter();
    }
}
