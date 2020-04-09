package org.covid19;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.covid19.Utils.zip;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AlertTextTests {

    @Test
    void deltaAlertLineSingleType() {
        final String expectedAlertText = "1 new case in Delhi\n";
        AtomicReference<String> actualAlertText = new AtomicReference<>("");

        StatewiseDelta delta = new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 0L, "", "Delhi");
        Covid19TelegramApp.buildDeltaAlertLine(actualAlertText, delta);

        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");
    }

    @Test
    void deltaAlertLineSingleTypePlural() {
        final String expectedAlertText = "10 new cases in Delhi\n";
        AtomicReference<String> actualAlertText = new AtomicReference<>("");

        StatewiseDelta delta = new StatewiseDelta(0L, 0L, 10L, 0L, 0L, 0L, "", "Delhi");
        Covid19TelegramApp.buildDeltaAlertLine(actualAlertText, delta);

        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");
    }

    @Test
    void deltaAlertLineMultipleTypes() {
        String expectedAlertText = "15 new cases, 9 recoveries in Maharashtra\n";
        AtomicReference<String> actualAlertText = new AtomicReference<>("");
        StatewiseDelta delta = new StatewiseDelta(9L, 0L, 15L, 0L, 0L, 0L, "", "Maharashtra");
        Covid19TelegramApp.buildDeltaAlertLine(actualAlertText, delta);
        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");

        expectedAlertText = "15 new cases, 4 deaths in Delhi\n";
        actualAlertText = new AtomicReference<>("");
        delta = new StatewiseDelta(0L, 4L, 15L, 0L, 0L, 0L, "", "Delhi");
        Covid19TelegramApp.buildDeltaAlertLine(actualAlertText, delta);
        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");

        expectedAlertText = "3 deaths, 9 recoveries in Delhi\n";
        actualAlertText = new AtomicReference<>("");
        delta = new StatewiseDelta(9L, 3L, 0L, 0L, 0L, 0L, "", "Delhi");
        Covid19TelegramApp.buildDeltaAlertLine(actualAlertText, delta);
        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");
    }

    @Test
    void summaryAlertBlock() {
        final String expectedSummaryBlock = "\n<b>Total</b>\n" +
                "<pre>\n" +
                "Total cases: (↑15) 5341\n" +
                "Recovered  : (↑9) 455\n" +
                "Deaths     : (↑4) 157\n" +
                "</pre>\n";
        AtomicReference<String> actualSummaryBlock = new AtomicReference<>("");

        List<StatewiseStats> stats = Collections.singletonList(new StatewiseStats("0", "5341", "157", "455", "Total", "TT", ""));
        List<StatewiseDelta> increments = Collections.singletonList(new StatewiseDelta(9L, 4L, 15L, 0L, 0L, 0L, "", "Total"));
        Covid19TelegramApp.buildSummaryAlertBlock(actualSummaryBlock, zip(stats, increments));

        assertEquals(expectedSummaryBlock, actualSummaryBlock.get(), "Summary block is not structured correctly!");
    }

    @Test
    void finalAlertText() {
        final String expectedFinalAlert = "<i>April 08, 12:04 AM</i>\n" +
                "\n" +
                "1 new case in Assam\n" +
                "9 new cases in Himachal Pradesh\n" +
                "\n" +
                "<b>Assam</b>\n" +
                "<pre>\n" +
                "Total cases: (↑1) 28\n" +
                "Recovered  : (↑0) 0\n" +
                "Deaths     : (↑0) 0\n" +
                "</pre>\n" +
                "\n" +
                "<b>Himachal Pradesh</b>\n" +
                "<pre>\n" +
                "Total cases: (↑9) 27\n" +
                "Recovered  : (↑0) 1\n" +
                "Deaths     : (↑0) 2\n" +
                "</pre>\n" +
                "\n" +
                "<b>Total</b>\n" +
                "<pre>\n" +
                "Total cases: (↑31) 5341\n" +
                "Recovered  : (↑8) 455\n" +
                "Deaths     : (↑3) 157\n" +
                "</pre>\n";

        List<StatewiseStats> stats = Arrays.asList(
                new StatewiseStats("0", "28", "0", "0", "Assam", "AS", ""),
                new StatewiseStats("0", "27", "2", "1", "Himachal Pradesh", "HP", ""),
                new StatewiseStats("0", "5341", "157", "455", "Total", "TT", ""));
        List<StatewiseDelta> increments = Arrays.asList(
                new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 0L, "08/04/2020 23:41:35", "Assam"),
                new StatewiseDelta(0L, 0L, 9L, 0L, 0L, 0L, "08/04/2020 00:04:28", "Himachal Pradesh"),
                new StatewiseDelta(8L, 3L, 31L, 0L, 0L, 0L, "08/04/2020 00:04:28", "Total"));
        List<StatewiseDelta> deltas = Arrays.asList(
                new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 0L, "08/04/2020 23:41:35", "Assam"),
                new StatewiseDelta(0L, 0L, 9L, 0L, 0L, 0L, "08/04/2020 00:04:28", "Himachal Pradesh"));

        final String actualFinalAlert = Covid19TelegramApp.buildStatewiseAlertText(zip(stats, increments), deltas);

        assertEquals(expectedFinalAlert, actualFinalAlert, "Summary block is not structured correctly!");
    }
}
