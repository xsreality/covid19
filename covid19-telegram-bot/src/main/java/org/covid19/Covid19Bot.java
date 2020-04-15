package org.covid19;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.telegram.abilitybots.api.bot.AbilityBot;
import org.telegram.abilitybots.api.db.DBContext;
import org.telegram.abilitybots.api.objects.Ability;
import org.telegram.abilitybots.api.objects.Locality;
import org.telegram.abilitybots.api.objects.Privacy;
import org.telegram.abilitybots.api.objects.Reply;
import org.telegram.abilitybots.api.objects.ReplyFlow;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.ReplyKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.KeyboardRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class Covid19Bot extends AbilityBot implements ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Bot.class);

    private static final String SUBSCRIBED_USERS = "SUBSCRIBED_USERS";

    private static final String[] NORTH_INDIAN_STATES = {
            "Delhi", "Jammu & Kashmir", "Himachal Pradesh", "Chandigarh",
            "Haryana", "Punjab", "Rajasthan", "Ladakh"
    };

    private static final String[] CENTRAL_INDIAN_STATES = {
            "Chhattisgarh", "Madhya Pradesh", "Uttar Pradesh", "Uttarakhand"
    };

    private static final String[] EAST_INDIAN_STATES = {
            "Bihar", "Jharkhand", "Odisha", "West Bengal"
    };

    private static final String[] NORTH_EAST_INDIAN_STATES = {
            "Arunachal Pradesh", "Assam", "Manipur", "Meghalaya",
            "Mizoram", "Nagaland", "Tripura", "Sikkim"
    };

    private static final String[] WEST_INDIAN_STATES = {
            "Goa", "Gujarat", "Maharashtra", "Dadra and Nagar Haveli", "Daman and Diu"
    };

    private static final String[] SOUTH_INDIAN_STATES = {
            "Andhra Pradesh", "Karnataka", "Kerala", "Puducherry",
            "Tamil Nadu", "Telangana", "Andaman and Nicobar Islands", "Lakshadweep"
    };


    private static Long CHAT_ID;
    private static Long CHANNEL_ID;
    private ApplicationContext appCtx;
    private KafkaTemplate<String, UserRequest> kafkaTemplate;

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
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    int userId = ctx.user().getId();

                    boolean newUser = !subscribedUsers.contains(String.valueOf(userId));
                    if (newUser) {
                        subscribedUsers.add(String.valueOf(userId));
                    }

                    String message = newUser ?
                            "Congratulations! You are now subscribed to Covid19 India Patient alerts! Send /stats to get statistics. Stay safe and keep social distancing!"
                            : "You are already subscribed to Covid19 India Patient alerts! Send /stats to get statistics.";
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
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    int userId = ctx.user().getId();

                    boolean existingUser = subscribedUsers.contains(String.valueOf(userId));
                    if (existingUser) {
                        subscribedUsers.remove(String.valueOf(userId));
                    }

                    String message = existingUser ?
                            "You have been unsubscribed from Covid19 India Patient alerts. Avoid information overload. Stay safe and keep social distancing!"
                            : "You are not yet subscribed to Covid19 India Patient alerts! Subscribe with /start";
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
    public Ability manuallyAdd() {
        return Ability
                .builder().name("add").info("Manually subscribe a user to Covid19 India patient alerts")
                .locality(Locality.USER).privacy(Privacy.CREATOR).input(1)
                .action(ctx -> {
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    if (subscribedUsers.contains(ctx.firstArg())) {
                        String message = "Already a subscribed user: " + ctx.firstArg();
                        silent.send(message, ctx.chatId());
                        return;
                    }
                    subscribedUsers.add(ctx.firstArg());
                    String message = "Manually subscribed user: " + ctx.firstArg();
                    silent.send(message, ctx.chatId());
                })
                .build();
    }

    @SuppressWarnings("unused")
    public Ability manuallyRemove() {
        return Ability
                .builder().name("remove").info("Manually unsubscribe a user to Covid19 India patient alerts")
                .locality(Locality.USER).privacy(Privacy.CREATOR).input(1)
                .action(ctx -> {
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    if (!subscribedUsers.contains(ctx.firstArg())) {
                        String message = "Not a subscribed user: " + ctx.firstArg();
                        silent.send(message, ctx.chatId());
                        return;
                    }
                    subscribedUsers.remove(ctx.firstArg());
                    String message = "Manually un-subscribed user: " + ctx.firstArg();
                    silent.send(message, ctx.chatId());
                })
                .build();
    }

    @SuppressWarnings("unused")
    public Ability listSubscribedUsers() {
        //noinspection unchecked
        return Ability
                .builder().name("list").info("List all subscribed users of Covid19 India patient alerts")
                .locality(Locality.USER).privacy(Privacy.CREATOR).input(0)
                .action(ctx -> {
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    AtomicReference<String> listOfUsers = new AtomicReference<>("");
                    subscribedUsers.forEach(user -> listOfUsers.accumulateAndGet(user, (s, s2) -> s + "\n" + s2));
                    String message = "List of users:\n" + listOfUsers;
                    silent.send(message, ctx.chatId());
                })
                .build();
    }

    public List<String> subscribedUsers() {
        return db.getList(SUBSCRIBED_USERS);
    }

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        this.appCtx = applicationContext;
        //noinspection unchecked
        kafkaTemplate = (KafkaTemplate<String, UserRequest>) appCtx.getBean("kafkaTemplate");
        kafkaTemplate.setProducerListener(new ProducerListener<String, UserRequest>() {
            @Override
            public void onSuccess(ProducerRecord<String, UserRequest> producerRecord, RecordMetadata recordMetadata) {
                LOG.info("Successfully produced user request for chatId {}, request {}", producerRecord.key(), producerRecord.value());
            }

            @Override
            public void onError(ProducerRecord<String, UserRequest> producerRecord, Exception exception) {
                LOG.error("Error producing record {}", producerRecord, exception);
            }
        });
    }

    @SuppressWarnings("unused")
    public ReplyFlow stateSelectionFlow() {
        final ReplyFlow northIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    SendMessage msg = buildNorthIndianStatesKeyboard(getChatId(upd));
                    silent.execute(msg);
                })
                .onlyIf(isMessage("North India"))
                .next(sendRequestToKafka(isNorthIndianState()))
                .build();

        final ReplyFlow centralIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    SendMessage msg = buildCentralIndianStatesKeyboard(getChatId(upd));
                    silent.execute(msg);
                })
                .onlyIf(isMessage("Central India"))
                .next(sendRequestToKafka(isCentralIndianState()))
                .build();

        final ReplyFlow eastIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    SendMessage msg = buildEastIndianStatesKeyboard(getChatId(upd));
                    silent.execute(msg);
                })
                .onlyIf(isMessage("East India"))
                .next(sendRequestToKafka(isEastIndianState()))
                .build();

        final ReplyFlow northEastIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    SendMessage msg = buildNorthEastIndianStatesKeyboard(getChatId(upd));
                    silent.execute(msg);
                })
                .onlyIf(isMessage("North East India"))
                .next(sendRequestToKafka(isNorthEastIndianState()))
                .build();

        final ReplyFlow westIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    SendMessage msg = buildWestIndianStatesKeyboard(getChatId(upd));
                    silent.execute(msg);
                })
                .onlyIf(isMessage("West India"))
                .next(sendRequestToKafka(isWestIndianState()))
                .build();

        final ReplyFlow southIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    SendMessage msg = buildSouthIndianStatesKeyboard(getChatId(upd));
                    silent.execute(msg);
                })
                .onlyIf(isMessage("South India"))
                .next(sendRequestToKafka(isSouthIndianState()))
                .build();

        return ReplyFlow.builder(db, 1)
                .action(update -> {
                    SendMessage msg = buildRegionKeyboard(getChatId(update));
                    silent.execute(msg);
                })
                .onlyIf(isMessage("/stats"))
                .next(northIndiaFlow)
                .next(centralIndiaFlow)
                .next(eastIndiaFlow)
                .next(northEastIndiaFlow)
                .next(westIndiaFlow)
                .next(southIndiaFlow)
                .next(sendRequestToKafka(isMessage("Total")))
                .build();
    }

    private Reply sendRequestToKafka(Predicate<Update> predicate) {
        return Reply.of(upd -> {
            kafkaTemplate.send("user-request", getChatId(upd), new UserRequest(getChatId(upd), upd.getMessage().getText()));
        }, predicate);
    }

    private String getChatId(Update update) {
        return String.valueOf(update.getMessage().getChatId());
    }

    private Predicate<Update> isNorthIndianState() {
        return upd -> Arrays.asList(NORTH_INDIAN_STATES).contains(upd.getMessage().getText());
    }

    private Predicate<Update> isCentralIndianState() {
        return upd -> Arrays.asList(CENTRAL_INDIAN_STATES).contains(upd.getMessage().getText());
    }

    private Predicate<Update> isEastIndianState() {
        return upd -> Arrays.asList(EAST_INDIAN_STATES).contains(upd.getMessage().getText());
    }

    private Predicate<Update> isNorthEastIndianState() {
        return upd -> Arrays.asList(NORTH_EAST_INDIAN_STATES).contains(upd.getMessage().getText());
    }

    private Predicate<Update> isWestIndianState() {
        return upd -> Arrays.asList(WEST_INDIAN_STATES).contains(upd.getMessage().getText());
    }

    private Predicate<Update> isSouthIndianState() {
        return upd -> Arrays.asList(SOUTH_INDIAN_STATES).contains(upd.getMessage().getText());
    }

    private Predicate<Update> isMessage(String msg) {
        return upd -> upd.getMessage().getText().equalsIgnoreCase(msg);
    }

    @NotNull
    private SendMessage buildRegionKeyboard(String chatId) {
        ReplyKeyboardMarkup markup = new ReplyKeyboardMarkup();
        markup.setOneTimeKeyboard(true);
        markup.setResizeKeyboard(true);
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a region");
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow row = new KeyboardRow();
        row.add("North India");
        row.add("Central India");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("East India");
        row.add("North East India");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("West India");
        row.add("South India");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Total");
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private SendMessage buildNorthIndianStatesKeyboard(String chatId) {
        ReplyKeyboardMarkup markup = new ReplyKeyboardMarkup();
        markup.setOneTimeKeyboard(true);
        markup.setResizeKeyboard(true);
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a state");
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow row = new KeyboardRow();
        row.add("Delhi");
        row.add("Jammu & Kashmir");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Himachal Pradesh");
        row.add("Chandigarh");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Haryana");
        row.add("Punjab");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Rajasthan");
        row.add("Ladakh");
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private SendMessage buildCentralIndianStatesKeyboard(String chatId) {
        ReplyKeyboardMarkup markup = new ReplyKeyboardMarkup();
        markup.setOneTimeKeyboard(true);
        markup.setResizeKeyboard(true);
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a state");
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow row = new KeyboardRow();
        row.add("Chhattisgarh");
        row.add("Madhya Pradesh");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Uttar Pradesh");
        row.add("Uttarakhand");
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private SendMessage buildEastIndianStatesKeyboard(String chatId) {
        ReplyKeyboardMarkup markup = new ReplyKeyboardMarkup();
        markup.setOneTimeKeyboard(true);
        markup.setResizeKeyboard(true);
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a state");
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow row = new KeyboardRow();
        row.add("Bihar");
        row.add("Jharkhand");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Odisha");
        row.add("West Bengal");
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private SendMessage buildNorthEastIndianStatesKeyboard(String chatId) {
        ReplyKeyboardMarkup markup = new ReplyKeyboardMarkup();
        markup.setOneTimeKeyboard(true);
        markup.setResizeKeyboard(true);
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a state");
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow row = new KeyboardRow();
        row.add("Arunachal Pradesh");
        row.add("Assam");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Manipur");
        row.add("Meghalaya");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Mizoram");
        row.add("Nagaland");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Tripura");
        row.add("Sikkim");
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private SendMessage buildWestIndianStatesKeyboard(String chatId) {
        ReplyKeyboardMarkup markup = new ReplyKeyboardMarkup();
        markup.setOneTimeKeyboard(true);
        markup.setResizeKeyboard(true);
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a state");
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow row = new KeyboardRow();
        row.add("Goa");
        row.add("Gujarat");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Maharashtra");
        row.add("Dadra and Nagar Haveli");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Daman and Diu");
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private SendMessage buildSouthIndianStatesKeyboard(String chatId) {
        ReplyKeyboardMarkup markup = new ReplyKeyboardMarkup();
        markup.setOneTimeKeyboard(true);
        markup.setResizeKeyboard(true);
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a state");
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow row = new KeyboardRow();
        row.add("Andhra Pradesh");
        row.add("Karnataka");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Kerala");
        row.add("Puducherry");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Tamil Nadu");
        row.add("Telangana");
        keyboard.add(row);

        row = new KeyboardRow();
        row.add("Andaman and Nicobar Islands");
        row.add("Lakshadweep");
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }
}
