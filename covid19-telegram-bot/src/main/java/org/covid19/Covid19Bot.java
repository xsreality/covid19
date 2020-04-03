package org.covid19;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.abilitybots.api.bot.AbilityBot;
import org.telegram.abilitybots.api.db.DBContext;
import org.telegram.abilitybots.api.objects.Ability;
import org.telegram.abilitybots.api.objects.Locality;
import org.telegram.abilitybots.api.objects.Privacy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Covid19Bot extends AbilityBot {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Bot.class);

    private static final String MAP_PATIENT_CHAT_HISTORY = "PATIENT_CHAT_HISTORY";

    private static Long CHAT_ID;
    private static Long CHANNEL_ID;

    protected Covid19Bot(String botToken, String botUsername, DBContext db, String creatorId, String channelId) {
        super(botToken, botUsername, db);
        CHAT_ID = Long.valueOf(creatorId);
        CHANNEL_ID = Long.valueOf(channelId);
    }

    @Override
    public int creatorId() {
        return CHAT_ID.intValue();
    }

    @SuppressWarnings("unused")
    public Ability sayHelloWorld() {
        return Ability
                .builder()
                .name("hello")
                .info("says hello world")
                .locality(Locality.ALL)
                .privacy(Privacy.PUBLIC)
                .action(messageContext ->
                        silent.send("Hello world from Java bot", CHAT_ID)
                                .ifPresent(message -> LOG.info("Returned msg id {} with username {}",
                                        message.getMessageId(), message.getChat().getUserName())))
                .build();
    }

    @SuppressWarnings("unused")
    public Ability subscribe() {
        return Ability
                .builder()
                .name("start")
                .info("Subscribe to Covid19 India patient alerts")
                .locality(Locality.ALL)
                .privacy(Privacy.PUBLIC)
                .input(0)
                .action(ctx -> {
                    Map<String, Map<String, String>> userPatientChatHistory = db.getMap(MAP_PATIENT_CHAT_HISTORY);
                    int userId = ctx.user().getId();

                    boolean newUser = userPatientChatHistory.putIfAbsent(String.valueOf(userId), new HashMap<>()) == null;

                    String message = newUser ?
                            "Congratulations! You are now subscribed to Covid19 India Patient alerts!"
                            : "You are already subscribed to Covid19 India Patient alerts!";
                    silent.send(message, ctx.chatId());
                })
                .post(ctx -> {
                    String message = String.format("User %s %s with id %d (re)subscribed to Covid19 India Patient alerts",
                            ctx.user().getFirstName(), ctx.user().getLastName(), ctx.user().getId());
                    silent.send(message, CHANNEL_ID);
                })
                .build();
    }

    @SuppressWarnings("unused")
    public Ability unsubscribe() {
        return Ability
                .builder()
                .name("stop")
                .info("Un-subscribe from Covid19 India patient alerts")
                .locality(Locality.ALL)
                .privacy(Privacy.PUBLIC)
                .input(0)
                .action(ctx -> {
                    Map<String, Map<String, String>> userPatientChatHistory = db.getMap("PATIENT_CHAT_HISTORY");
                    int userId = ctx.user().getId();


                    boolean newUser = userPatientChatHistory.remove(String.valueOf(userId)) == null;

                    String message = newUser ?
                            "You are not yet subscribed to Covid19 India Patient alerts! Subscribe with /start"
                            : "You have been unsubscribed from Covid19 India Patient alerts. Sorry to see you go.";
                    silent.send(message, ctx.chatId());
                })
                .post(ctx -> {
                    String message = String.format("User %s with id %d unsubscribed from Covid19 India Patient alerts",
                            ctx.user().getUserName(), ctx.user().getId());
                    silent.send(message, CHANNEL_ID);
                })
                .build();
    }

    @SuppressWarnings("unused")
    public Ability dbInfo() {
        return Ability
                .builder()
                .name("dbsummary")
                .info("Subscribe to Covid19 India patient alerts")
                .locality(Locality.USER)
                .privacy(Privacy.CREATOR)
                .input(0)
                .action(ctx -> {
                    final String summary = db.summary();
                    String message = String.format("DB summary is %s", summary);
                    silent.send(message, ctx.chatId());
                })
                .build();
    }

    @SuppressWarnings("unused")
    public Ability patientChatHistory() {
        return Ability
                .builder()
                .name("history")
                .info("Subscribe to Covid19 India patient alerts")
                .locality(Locality.USER)
                .privacy(Privacy.CREATOR)
                .input(1)
                .action(ctx -> {
                    Map<String, Map<String, String>> userPatientChatHistory = db.getMap(MAP_PATIENT_CHAT_HISTORY);
                    int userId = ctx.user().getId();

                    final Map<String, String> patientChatHistory = userPatientChatHistory.get(ctx.firstArg());

                    if (Objects.isNull(patientChatHistory)) {
                        String message = "Not a subscribed user: " + ctx.firstArg();
                        silent.send(message, ctx.chatId());
                        return;
                    }

                    final List<String> messages = new ArrayList<>();
                    patientChatHistory.forEach((patientNumber, messageId) -> {
                        messages.add(String.format("%s : %s", patientNumber, messageId));
                    });

                    String message = String.join("\n", messages);
                    if (message.isEmpty()) {
                        message = "No history found for " + ctx.firstArg();
                    }

                    silent.send(message, ctx.chatId());
                })
                .build();
    }


    public Map<String, Map<String, String>> userPatientChatHistory() {
        return db.getMap(MAP_PATIENT_CHAT_HISTORY);
    }

    public void updateChatHistory(Map<String, Map<String, String>> history) {
        final Map<String, Map<String, String>> chatHistory = db.getMap(MAP_PATIENT_CHAT_HISTORY);
        history.keySet().forEach(key -> chatHistory.put(key, history.get(key)));
        }
}
