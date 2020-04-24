package org.covid19;

import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Chat;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.covid19.Utils.friendlyTime;
import static org.covid19.Utils.initStateCodes;
import static org.covid19.Utils.zip;

@Slf4j
public class TelegramUtils {
    final static Map<String, String> stateCodes;

    static {
        stateCodes = initStateCodes();
    }

    static String buildAlertText(boolean update, PatientAndMessage patientAndMessage) {
        PatientInfo patientInfo = patientAndMessage.getPatientInfo();
        String alertText;
        if (update) {
            alertText = String.format("Update on patient #%s\n\n", patientInfo.getPatientNumber());
        } else {
            alertText = String.format("Patient #%s\n\n", patientInfo.getPatientNumber());
        }

        alertText = alertText.concat(String.format("Status: %s\n\n", patientInfo.getCurrentStatus()));

        if (!isNullOrEmpty(patientInfo.getAgeBracket())) {
            alertText = alertText.concat(String.format("Age: %s\n", patientInfo.getAgeBracket()));
        }

        if (!isNullOrEmpty(patientInfo.getGender())) {
            String gender = "M".equalsIgnoreCase(patientInfo.getGender()) ? "Male" : "Female";
            alertText = alertText.concat(String.format("Gender: %s\n", gender));
        }

        alertText = alertText.concat(String.format("Place: %s %s\n", patientInfo.getDetectedCity(), patientInfo.getDetectedState()));

        if (!isNullOrEmpty(patientInfo.getD180G())) {
            alertText = alertText.concat(String.format("Possible cause: %s\n", patientInfo.getD180G()));
        } else if (!isNullOrEmpty(patientInfo.getNotes())) {
            alertText = alertText.concat(String.format("Possible cause: %s\n", patientInfo.getNotes()));
        } else if (!isNullOrEmpty(patientInfo.getBackupNotes())) {
            alertText = alertText.concat(String.format("Possible cause: %s\n", patientInfo.getBackupNotes()));
        }

        if (!isNullOrEmpty(patientInfo.getDateAnnounced())) {
            alertText = alertText.concat(String.format("Announced: %s\n", patientInfo.getDateAnnounced()));
        }

        if (!isNullOrEmpty(patientInfo.getSource1())
                || !isNullOrEmpty(patientInfo.getSource2())
                || !isNullOrEmpty(patientInfo.getSource3())) {
            alertText = alertText.concat("\nSources:\n");
        }

        if (!isNullOrEmpty(patientInfo.getSource1())) {
            alertText = alertText.concat(String.format("%s\n", patientInfo.getSource1()));
        }
        if (!isNullOrEmpty(patientInfo.getSource2())) {
            alertText = alertText.concat(String.format("%s\n", patientInfo.getSource2()));
        }
        if (!isNullOrEmpty(patientInfo.getSource3())) {
            alertText = alertText.concat(String.format("%s\n", patientInfo.getSource3()));
        }

        LOG.info("Alert Text built for patient #{}:\n{}", patientInfo.getPatientNumber(), alertText);

        return alertText;
    }

    static void sendTelegramAlert(Covid19Bot bot, String chatId, String alertText, Integer replyId, boolean notification) {
        try {
            Thread.sleep(50);  // to avoid hitting Telegram rate limits
            SendMessage telegramMessage = new SendMessage()
                    .setChatId(chatId)
                    .setText(alertText)
                    .enableHtml(true)
                    .setReplyToMessageId(replyId);

            telegramMessage = notification ? telegramMessage.enableNotification() : telegramMessage.disableNotification();

            bot.execute(telegramMessage);
        } catch (TelegramApiException | InterruptedException e) {
            LOG.error("Unable to send Telegram alert to user {}, with error {}", chatId, e.getMessage());
        }
    }

    static String buildStatewiseAlertText(String lastUpdated, List<StatewiseDelta> deltas, List<StatewiseDelta> dailies, Map<String, String> doublingRates) {
        AtomicReference<String> alertText = new AtomicReference<>("");
        deltas.forEach(delta -> buildDeltaAlertLine(alertText, delta));
        if (alertText.get().isEmpty() || "\n".equalsIgnoreCase(alertText.get())) {
            LOG.info("No useful update to alert on. Skipping...");
            return "";
        }
        buildSummaryAlertBlock(alertText, deltas, dailies, doublingRates);
        String finalText = String.format("<i>%s</i>\n\n%s", lastUpdated, alertText.get());
        LOG.info("Statewise Alert text generated:\n{}", finalText);
        return finalText;
    }

    static void fireStatewiseTelegramAlert(Covid19Bot covid19Bot, String alertText) {
        if (isNull(alertText) || alertText.isEmpty()) {
            return; // skip sending alert
        }
        final List<String> subscribedUsers = covid19Bot.subscribedUsers();
        subscribedUsers.forEach(subscriber -> {
            LOG.info("Sending statewise updates to {}", subscriber);
            sendTelegramAlert(covid19Bot, subscriber, alertText, null, true);
        });
    }

    static void buildSummaryAlertBlock(AtomicReference<String> updateText, List<StatewiseDelta> deltas,
                                       List<StatewiseDelta> dailies, Map<String, String> doublingRates) {
        zip(deltas, dailies).forEach(pair -> {
            StatewiseDelta delta = pair.getKey();
            StatewiseDelta daily = pair.getValue();
            String statText = String.format("\n<b>%s</b>\n" +
                            "<pre>\n" +
                            "Total cases  : (↑%s) %s\n" +
                            "Active       : (↑%s) %s\n" +
                            "Recovered    : (↑%s) %s\n" +
                            "Deaths       : (↑%s) %s\n" +
                            "Doubling rate: %s days\n" +
                            "</pre>\n",
                    delta.getState(),
                    nonNull(daily) ? daily.getDeltaConfirmed() : "", delta.getCurrentConfirmed(),
                    nonNull(daily) ? daily.getDeltaConfirmed() - daily.getDeltaRecovered() - daily.getDeltaDeaths() : "", delta.getCurrentConfirmed() - delta.getCurrentRecovered() - delta.getCurrentDeaths(),
                    nonNull(daily) ? daily.getDeltaRecovered() : "", delta.getCurrentRecovered(),
                    nonNull(daily) ? daily.getDeltaDeaths() : "", delta.getCurrentDeaths(),
                    doublingRates.get(delta.getState()));
            updateText.accumulateAndGet(statText, (current, update) -> current + update);
        });
    }

    static void buildDeltaAlertLine(AtomicReference<String> updateText, StatewiseDelta delta) {
        // skip total
        if ("total".equalsIgnoreCase(delta.getState())) {
            return;
        }

        boolean confirmed = false, deaths = false, include = false;
        String textLine = "";
        if (delta.getDeltaConfirmed() > 0L) {
            include = true;
            confirmed = true;
            textLine = textLine.concat(String.format("%d new %s",
                    delta.getDeltaConfirmed(),
                    delta.getDeltaConfirmed() == 1L ? "case" : "cases"));
        }
        if (delta.getDeltaDeaths() > 0L) {
            deaths = true;
            include = true;
            textLine = textLine.concat(String.format("%s%d %s",
                    confirmed ? ", " : "",
                    delta.getDeltaDeaths(),
                    delta.getDeltaDeaths() == 1L ? "death" : "deaths"));
        }
        if (delta.getDeltaRecovered() > 0L) {
            include = true;
            textLine = textLine.concat(String.format("%s%d %s",
                    confirmed || deaths ? ", " : "",
                    delta.getDeltaRecovered(),
                    delta.getDeltaRecovered() == 1L ? "recovery" : "recoveries"));
        }
        if (include) {
            textLine = textLine.concat(String.format(" in %s\n",
                    delta.getState()));
        }
        updateText.accumulateAndGet(textLine, (current, update) -> current + update);
    }

    static String buildStateSummaryAlertText(List<StatewiseDelta> sortedStats, String lastUpdated, boolean daily) {
        StatewiseDelta total = new StatewiseDelta();
        String text = String.format("<i>%s</i>\n\n", friendlyTime(lastUpdated));
        text = text.concat("Summary of all affected Indian States\n\n");
        text = text.concat("<pre>\n");
        text = text.concat("State|  Conf|  Rec.| Died\n");
        text = text.concat("-------------------------\n");
        for (StatewiseDelta stat : sortedStats) {
            if ("Total".equalsIgnoreCase(stat.getState())) {
                total = stat;
                continue; // show total at the end
            }
            if (daily) {
                if (stat.getDeltaConfirmed() < 1L && stat.getDeltaRecovered() < 1L && stat.getDeltaDeaths() < 1L) {
                    continue; // skip states with zero stats
                }
            } else {
                if (stat.getCurrentConfirmed() < 1L && stat.getCurrentRecovered() < 1L && stat.getCurrentDeaths() < 1L) {
                    continue; // skip states with zero stats
                }
            }
            text = text.concat(String.format("%-5s|%6s|%6s|%5s\n",
                    stateCodes.get(stat.getState()),
                    daily ? stat.getDeltaConfirmed() : stat.getCurrentConfirmed(),
                    daily ? stat.getDeltaRecovered() : stat.getCurrentRecovered(),
                    daily ? stat.getDeltaDeaths() : stat.getCurrentDeaths()));
        }
        text = text.concat("-------------------------\n");
        text = text.concat(String.format("%-5s|%6s|%6s|%5s\n",
                stateCodes.get(total.getState()),
                daily ? total.getDeltaConfirmed() : total.getCurrentConfirmed(),
                daily ? total.getDeltaRecovered() : total.getCurrentRecovered(),
                daily ? total.getDeltaDeaths() : total.getCurrentDeaths()));
        text = text.concat("</pre>");
        return text;
    }

    static String translateName(Chat chat) {
        if (nonNull(chat.getFirstName())) {
            if (nonNull(chat.getLastName())) {
                return chat.getFirstName() + " " + chat.getLastName();
            }
            return chat.getFirstName();
        } else if (nonNull(chat.getUserName())) {
            return chat.getUserName();
        }
        return "";
    }
}
