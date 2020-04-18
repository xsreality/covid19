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
import org.telegram.abilitybots.api.objects.Reply;
import org.telegram.abilitybots.api.objects.ReplyFlow;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.covid19.TelegramUtils.translateName;
import static org.telegram.abilitybots.api.objects.Locality.ALL;
import static org.telegram.abilitybots.api.objects.Locality.USER;
import static org.telegram.abilitybots.api.objects.Privacy.CREATOR;
import static org.telegram.abilitybots.api.objects.Privacy.PUBLIC;

@SuppressWarnings("unused")
public class Covid19Bot extends AbilityBot implements ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Bot.class);

    private static final String SUBSCRIBED_USERS = "SUBSCRIBED_USERS";

    private static final String[] NORTH_INDIAN_STATES = {
            "Delhi", "Jammu and Kashmir", "Himachal Pradesh", "Chandigarh",
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
    private KafkaTemplate<String, UserRequest> userRequestKafkaTemplate;
    private KafkaTemplate<String, UserPrefs> userPrefsKafkaTemplate;
    private StateStoresManager storesManager;

    protected Covid19Bot(String botToken, String botUsername, DBContext db, String creatorId, String channelId) {
        super(botToken, botUsername, db);
        CHAT_ID = Long.valueOf(creatorId);
        CHANNEL_ID = Long.valueOf(channelId);
    }

    @Override
    public int creatorId() {
        return CHAT_ID.intValue();
    }

    public Ability oneTimeMigrate() {
        return Ability
                .builder()
                .name("migrate").info("Migrate all subscribers to Kafka")
                .locality(USER).privacy(CREATOR)
                .action(msgCtx -> {
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    subscribedUsers.forEach(user -> {
                        userPrefsKafkaTemplate.send("user-preferences", String.valueOf(user),
                                new UserPrefs(String.valueOf(user), emptyList(), true));
                    });
                })
                .post(msgCtx -> {
                    silent.send("All users migrated to Kafka", CHANNEL_ID);
                })
                .build();
    }

    @SuppressWarnings("unused")
    public Ability sayHelloWorld() {
        return Ability
                .builder()
                .name("hello")
                .info("says hello world")
                .locality(ALL)
                .privacy(PUBLIC)
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
                .locality(ALL)
                .privacy(PUBLIC)
                .input(0)
                .action(ctx -> {
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    int userId = ctx.user().getId();

                    boolean newUser = !subscribedUsers.contains(String.valueOf(userId));
                    if (newUser) {
                        subscribedUsers.add(String.valueOf(userId));
                    }

                    // send a message to kafka user-preferences
                    userPrefsKafkaTemplate.send("user-preferences", String.valueOf(userId),
                            new UserPrefs(String.valueOf(userId), emptyList(), true));

                    String message = newUser ?
                            "Congratulations! You are now subscribed to Covid19 India Patient alerts! Send /stats to get statistics. Stay safe and keep social distancing!"
                            : "You are already subscribed to Covid19 India Patient alerts! Send /stats to get statistics.";
                    silent.send(message, ctx.chatId());
                })
                .post(ctx -> {
                    String message = String.format("User %s (%d) subscribed to Covid19 India Patient alerts",
                            translateName(ctx.update().getMessage().getChat()), ctx.user().getId());
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
                .locality(ALL)
                .privacy(PUBLIC)
                .input(0)
                .action(ctx -> {
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    int userId = ctx.user().getId();

                    boolean existingUser = subscribedUsers.contains(String.valueOf(userId));
                    if (existingUser) {
                        subscribedUsers.remove(String.valueOf(userId));
                    }

                    // send a message to update kafka user-preferences
                    userPrefsKafkaTemplate.send("user-preferences", String.valueOf(userId),
                            new UserPrefs(String.valueOf(userId), emptyList(), false));


                    String message = existingUser ?
                            "You have been unsubscribed from Covid19 India Patient alerts. Avoid information overload. Stay safe and keep social distancing!"
                            : "You are not yet subscribed to Covid19 India Patient alerts! Subscribe with /start";
                    silent.send(message, ctx.chatId());
                })
                .post(ctx -> {
                    String message = String.format("User %s (%d) unsubscribed from Covid19 India Patient alerts",
                            translateName(ctx.update().getMessage().getChat()), ctx.user().getId());
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
                .locality(USER)
                .privacy(CREATOR)
                .input(0)
                .action(ctx -> {
                    final String summary = db.summary();
                    String message = String.format("DB summary is %s", summary);
                    silent.send(message, ctx.chatId());
                })
                .build();
    }

    public Ability manuallyAdd() {
        return Ability
                .builder().name("add").info("Manually subscribe a user to Covid19 India patient alerts")
                .locality(USER).privacy(CREATOR).input(1)
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

    public Ability manuallyRemove() {
        return Ability
                .builder().name("remove").info("Manually unsubscribe a user to Covid19 India patient alerts")
                .locality(USER).privacy(CREATOR).input(1)
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

    public Ability listSubscribedUsers() {
        return Ability
                .builder().name("list").info("List all subscribed users of Covid19 India patient alerts")
                .locality(USER).privacy(CREATOR).input(0)
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

        this.storesManager = (StateStoresManager) appCtx.getBean("stateStoresManager");

        //noinspection unchecked
        userRequestKafkaTemplate = (KafkaTemplate<String, UserRequest>) appCtx.getBean("userRequestKafkaTemplate");
        userRequestKafkaTemplate.setProducerListener(new ProducerListener<String, UserRequest>() {
            @Override
            public void onSuccess(ProducerRecord<String, UserRequest> producerRecord, RecordMetadata recordMetadata) {
                LOG.info("Successfully produced user request for chatId {}, request {}", producerRecord.key(), producerRecord.value());
            }

            @Override
            public void onError(ProducerRecord<String, UserRequest> producerRecord, Exception exception) {
                LOG.error("Error producing record {}", producerRecord, exception);
            }
        });

        //noinspection unchecked
        userPrefsKafkaTemplate = (KafkaTemplate<String, UserPrefs>) appCtx.getBean("userPrefsKafkaTemplate");
        userPrefsKafkaTemplate.setProducerListener(new ProducerListener<String, UserPrefs>() {
            @Override
            public void onSuccess(ProducerRecord<String, UserPrefs> producerRecord, RecordMetadata recordMetadata) {
                LOG.info("Successfully produced user request for chatId {}, request {}", producerRecord.key(), producerRecord.value());
            }

            @Override
            public void onError(ProducerRecord<String, UserPrefs> producerRecord, Exception exception) {
                LOG.error("Error producing record {}", producerRecord, exception);
            }
        });
    }

    public Ability clearMyState() {
        return Ability.builder()
                .name("clearmystate").info("Clear preferred state (if set). You will receive updates for every Indian state.")
                .locality(ALL).privacy(PUBLIC).input(0)
                .action(ctx -> {
                    String chatId = String.valueOf(ctx.chatId());
                    UserPrefs prefs = storesManager.prefsForUser(chatId);

                    String message = "";
                    if (prefs.getMyStates().isEmpty()) {
                        message = "You do not have any preferred state set. Send /mystate to choose a preferred state.";
                    } else {
                        userPrefsKafkaTemplate.send("user-preferences", chatId, new UserPrefs(chatId, emptyList(), true));
                        String currentState = prefs.getMyStates().get(0);
                        message = String.format("Your preferred state (%s) has been removed. You will start receiving updates of all Indian states.", currentState);
                    }
                    silent.send(message, ctx.chatId());
                })
                .post(ctx -> {
                    // send update to bot channel
                    String message = String.format("User %s (%d) reset their preferred state.",
                            translateName(ctx.update().getMessage().getChat()), ctx.chatId());
                    silent.send(message, CHANNEL_ID);
                })
                .build();
    }

    public Ability getMyState() {
        return Ability.builder()
                .name("getmystate").info("Get my preferred state")
                .locality(ALL).privacy(PUBLIC).input(0)
                .action(ctx -> {
                    String chatId = String.valueOf(ctx.chatId());
                    UserPrefs prefs = storesManager.prefsForUser(chatId);

                    String message = "";
                    if (prefs.getMyStates().isEmpty()) {
                        message = "You do not have any preferred state set. Send /mystate to choose a preferred state.";
                    } else {
                        message = String.format("Your preferred state is %s.\n\n" +
                                "To clear your preferred state and receive updates of all states, send /clearmystate", prefs.getMyStates().get(0));
                    }
                    silent.send(message, ctx.chatId());
                }).build();
    }

    public ReplyFlow myStateFlow() {
        final ReplyFlow northIndiaFlow = ReplyFlow.builder(db, 11)
                .action(upd -> {
                    EditMessageText msg = buildNorthIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("North India"))
                .next(sendUserPrefsToKafka(isAnyState()))
                .build();

        final ReplyFlow centralIndiaFlow = ReplyFlow.builder(db, 12)
                .action(upd -> {
                    EditMessageText msg = buildCentralIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("Central India"))
                .next(sendUserPrefsToKafka(isAnyState()))
                .build();

        final ReplyFlow eastIndiaFlow = ReplyFlow.builder(db, 13)
                .action(upd -> {
                    EditMessageText msg = buildEastIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("East India"))
                .next(sendUserPrefsToKafka(isAnyState()))
                .build();

        final ReplyFlow northEastIndiaFlow = ReplyFlow.builder(db, 14)
                .action(upd -> {
                    EditMessageText msg = buildNorthEastIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("North East India"))
                .next(sendUserPrefsToKafka(isAnyState()))
                .build();

        final ReplyFlow westIndiaFlow = ReplyFlow.builder(db, 15)
                .action(upd -> {
                    EditMessageText msg = buildWestIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("West India"))
                .next(sendUserPrefsToKafka(isAnyState()))
                .build();

        final ReplyFlow southIndiaFlow = ReplyFlow.builder(db, 16)
                .action(upd -> {
                    EditMessageText msg = buildSouthIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("South India"))
                .next(sendUserPrefsToKafka(isAnyState()))
                .build();

        return ReplyFlow.builder(db, 10)
                .action(update -> {
                    SendMessage msg = buildRegionKeyboard(getChatId(update), false);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("/mystate"))
                .next(northIndiaFlow)
                .next(centralIndiaFlow)
                .next(eastIndiaFlow)
                .next(northEastIndiaFlow)
                .next(westIndiaFlow)
                .next(southIndiaFlow)
                .build();
    }

    public ReplyFlow requestAnyStateFlow() {
        final ReplyFlow northIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    EditMessageText msg = buildNorthIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("North India"))
                .next(sendRequestToKafka(isAnyState()))
                .build();

        final ReplyFlow centralIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    EditMessageText msg = buildCentralIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("Central India"))
                .next(sendRequestToKafka(isAnyState()))
                .build();

        final ReplyFlow eastIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    EditMessageText msg = buildEastIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("East India"))
                .next(sendRequestToKafka(isAnyState()))
                .build();

        final ReplyFlow northEastIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    EditMessageText msg = buildNorthEastIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("North East India"))
                .next(sendRequestToKafka(isAnyState()))
                .build();

        final ReplyFlow westIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    EditMessageText msg = buildWestIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("West India"))
                .next(sendRequestToKafka(isAnyState()))
                .build();

        final ReplyFlow southIndiaFlow = ReplyFlow.builder(db, 2)
                .action(upd -> {
                    EditMessageText msg = buildSouthIndianStatesKeyboard(upd);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("South India"))
                .next(sendRequestToKafka(isAnyState()))
                .build();

        return ReplyFlow.builder(db, 1)
                .action(update -> {
                    SendMessage msg = buildRegionKeyboard(getChatId(update), true);
                    silent.execute(msg);
                })
                .onlyIf(isCallbackOrMessage("/stats"))
                .next(northIndiaFlow)
                .next(centralIndiaFlow)
                .next(eastIndiaFlow)
                .next(northEastIndiaFlow)
                .next(westIndiaFlow)
                .next(southIndiaFlow)
                .next(sendRequestToKafka(isCallbackOrMessage("Total")))
                .next(sendRequestToKafka(isCallbackOrMessage("Summary")))
                .build();
    }

    private Reply sendRequestToKafka(Predicate<Update> predicate) {
        return Reply.of(upd -> {
            String chatId = String.valueOf(upd.getCallbackQuery().getMessage().getChatId());
            String state = upd.getCallbackQuery().getData();

            userRequestKafkaTemplate.send("user-request", getChatId(upd), new UserRequest(chatId, state));

            EditMessageText msg = new EditMessageText();
            msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
            msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
            msg.setText("Fetching latest status of " + state + "...");
            silent.execute(msg);

            // send an update to Bot channel
            String channelMsg = String.format("User %s (%s) requested stats for %s", translateName(upd.getCallbackQuery().getMessage().getChat()), chatId, state);
            silent.send(channelMsg, CHANNEL_ID);
        }, predicate);
    }

    private Reply sendUserPrefsToKafka(Predicate<Update> predicate) {
        return Reply.of(upd -> {
            String chatId = String.valueOf(upd.getCallbackQuery().getMessage().getChatId());
            String state = upd.getCallbackQuery().getData();

            userPrefsKafkaTemplate.send("user-preferences", getChatId(upd), new UserPrefs(chatId, singletonList(state), true));

            EditMessageText msg = new EditMessageText();
            msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
            msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
            msg.setText(String.format("Your preferred state is set to %s. " +
                    "You will receive updates about %s only. " +
                    "To cancel this, send /clearmystate", state, state));
            silent.execute(msg);

            // send an update to Bot channel
            String channelMsg = String.format("User %s (%s) set preferred state to %s", translateName(upd.getCallbackQuery().getMessage().getChat()), chatId, state);
            silent.send(channelMsg, CHANNEL_ID);

        }, predicate);
    }

    private String getChatId(Update update) {
        return update.hasMessage() ?
                String.valueOf(update.getMessage().getChatId()) :
                String.valueOf(update.getCallbackQuery().getMessage().getChatId());
    }

    private Predicate<Update> isAnyState() {
        return upd -> {
            if (upd.hasCallbackQuery()) {
                String state = upd.getCallbackQuery().getData();
                return asList(NORTH_INDIAN_STATES).contains(state) || asList(CENTRAL_INDIAN_STATES).contains(state)
                        || asList(NORTH_EAST_INDIAN_STATES).contains(state) || asList(EAST_INDIAN_STATES).contains(state)
                        || asList(WEST_INDIAN_STATES).contains(state) || asList(SOUTH_INDIAN_STATES).contains(state);
            }
            return false;
        };
    }

    private Predicate<Update> isCallbackOrMessage(String msg) {
        return upd -> (upd.hasMessage() && upd.getMessage().getText().equalsIgnoreCase(msg)) ||
                (upd.hasCallbackQuery() && upd.getCallbackQuery().getData().equalsIgnoreCase(msg));
    }

    @NotNull
    private SendMessage buildRegionKeyboard(String chatId, boolean showTotalSummary) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText("Choose a region");
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        List<InlineKeyboardButton> row = new ArrayList<>();
        row.add(new InlineKeyboardButton().setText("North India").setCallbackData("North India"));
        row.add(new InlineKeyboardButton().setText("Central India").setCallbackData("Central India"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton().setText("East India").setCallbackData("East India"));
        row.add(new InlineKeyboardButton().setText("North East India").setCallbackData("North East India"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton().setText("West India").setCallbackData("West India"));
        row.add(new InlineKeyboardButton().setText("South India").setCallbackData("South India"));
        keyboard.add(row);

        if (showTotalSummary) {
            row = new ArrayList<>();
            row.add(new InlineKeyboardButton().setText("Total").setCallbackData("Total"));
            row.add(new InlineKeyboardButton().setText("Summary").setCallbackData("Summary"));
            keyboard.add(row);
        }

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private EditMessageText buildNorthIndianStatesKeyboard(Update upd) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        EditMessageText msg = new EditMessageText();
        msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
        msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
        msg.setText("Choose a state");
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        List<InlineKeyboardButton> row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Delhi").setCallbackData("Delhi"));
        row.add(new InlineKeyboardButton("Jammu and Kashmir").setCallbackData("Jammu and Kashmir"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Himachal Pradesh").setCallbackData("Himachal Pradesh"));
        row.add(new InlineKeyboardButton("Chandigarh").setCallbackData("Chandigarh"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Haryana").setCallbackData("Haryana"));
        row.add(new InlineKeyboardButton("Punjab").setCallbackData("Punjab"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Rajasthan").setCallbackData("Rajasthan"));
        row.add(new InlineKeyboardButton("Ladakh").setCallbackData("Ladakh"));
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private EditMessageText buildCentralIndianStatesKeyboard(Update upd) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        EditMessageText msg = new EditMessageText();
        msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
        msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
        msg.setText("Choose a state");
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        List<InlineKeyboardButton> row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Chhattisgarh").setCallbackData("Chhattisgarh"));
        row.add(new InlineKeyboardButton("Madhya Pradesh").setCallbackData("Madhya Pradesh"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Uttar Pradesh").setCallbackData("Uttar Pradesh"));
        row.add(new InlineKeyboardButton("Uttarakhand").setCallbackData("Uttarakhand"));
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private EditMessageText buildEastIndianStatesKeyboard(Update upd) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        EditMessageText msg = new EditMessageText();
        msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
        msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
        msg.setText("Choose a state");
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        List<InlineKeyboardButton> row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Bihar").setCallbackData("Bihar"));
        row.add(new InlineKeyboardButton("Jharkhand").setCallbackData("Jharkhand"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Odisha").setCallbackData("Odisha"));
        row.add(new InlineKeyboardButton("West Bengal").setCallbackData("West Bengal"));
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private EditMessageText buildNorthEastIndianStatesKeyboard(Update upd) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        EditMessageText msg = new EditMessageText();
        msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
        msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
        msg.setText("Choose a state");
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        List<InlineKeyboardButton> row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Arunachal Pradesh").setCallbackData("Arunachal Pradesh"));
        row.add(new InlineKeyboardButton("Assam").setCallbackData("Assam"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Manipur").setCallbackData("Manipur"));
        row.add(new InlineKeyboardButton("Meghalaya").setCallbackData("Meghalaya"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Mizoram").setCallbackData("Mizoram"));
        row.add(new InlineKeyboardButton("Nagaland").setCallbackData("Nagaland"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Tripura").setCallbackData("Tripura"));
        row.add(new InlineKeyboardButton("Sikkim").setCallbackData("Sikkim"));
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private EditMessageText buildWestIndianStatesKeyboard(Update upd) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        EditMessageText msg = new EditMessageText();
        msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
        msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
        msg.setText("Choose a state");
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        List<InlineKeyboardButton> row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Goa").setCallbackData("Goa"));
        row.add(new InlineKeyboardButton("Gujarat").setCallbackData("Gujarat"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Maharashtra").setCallbackData("Maharashtra"));
        row.add(new InlineKeyboardButton("Dadra and Nagar Haveli").setCallbackData("Dadra and Nagar Haveli"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Daman and Diu").setCallbackData("Daman and Diu"));
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }

    private EditMessageText buildSouthIndianStatesKeyboard(Update upd) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        EditMessageText msg = new EditMessageText();
        msg.setChatId(upd.getCallbackQuery().getMessage().getChatId());
        msg.setMessageId(upd.getCallbackQuery().getMessage().getMessageId());
        msg.setText("Choose a state");
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        List<InlineKeyboardButton> row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Andhra Pradesh").setCallbackData("Andhra Pradesh"));
        row.add(new InlineKeyboardButton("Karnataka").setCallbackData("Karnataka"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Kerala").setCallbackData("Kerala"));
        row.add(new InlineKeyboardButton("Puducherry").setCallbackData("Puducherry"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Tamil Nadu").setCallbackData("Tamil Nadu"));
        row.add(new InlineKeyboardButton("Telangana").setCallbackData("Telangana"));
        keyboard.add(row);

        row = new ArrayList<>();
        row.add(new InlineKeyboardButton("Andaman and Nicobar Islands").setCallbackData("Andaman and Nicobar Islands"));
        row.add(new InlineKeyboardButton("Lakshadweep").setCallbackData("Lakshadweep"));
        keyboard.add(row);

        markup.setKeyboard(keyboard);
        msg.setReplyMarkup(markup);
        return msg;
    }
}
