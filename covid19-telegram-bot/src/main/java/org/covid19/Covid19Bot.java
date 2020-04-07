package org.covid19;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.abilitybots.api.bot.AbilityBot;
import org.telegram.abilitybots.api.db.DBContext;
import org.telegram.abilitybots.api.objects.Ability;
import org.telegram.abilitybots.api.objects.Locality;
import org.telegram.abilitybots.api.objects.Privacy;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Covid19Bot extends AbilityBot {
    private static final Logger LOG = LoggerFactory.getLogger(Covid19Bot.class);

    private static final String SUBSCRIBED_USERS = "SUBSCRIBED_USERS";

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
                    List<String> subscribedUsers = db.getList(SUBSCRIBED_USERS);
                    int userId = ctx.user().getId();

                    boolean newUser = !subscribedUsers.contains(String.valueOf(userId));
                    if (newUser) {
                        subscribedUsers.add(String.valueOf(userId));
                    }

                    String message = newUser ?
                            "Congratulations! You are now subscribed to Covid19 India Patient alerts! Stay safe and keep social distancing!"
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

}
