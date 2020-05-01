package org.covid19;

import org.covid19.district.DistrictwiseData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.covid19.bot.BotUtils.buildDeltaAlertLine;
import static org.covid19.bot.BotUtils.buildStatewiseAlertText;
import static org.covid19.bot.BotUtils.buildSummaryAlertBlock;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AlertTextTests {

    @Test
    void deltaAlertLineSingleType() {
        final String expectedAlertText = "1 new case in Delhi\n";
        AtomicReference<String> actualAlertText = new AtomicReference<>("");

        StatewiseDelta delta = new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 0L, "", "Delhi");
        buildDeltaAlertLine(actualAlertText, delta);

        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");
    }

    @Test
    void deltaAlertLineSingleTypePlural() {
        final String expectedAlertText = "10 new cases in Delhi\n";
        AtomicReference<String> actualAlertText = new AtomicReference<>("");

        StatewiseDelta delta = new StatewiseDelta(0L, 0L, 10L, 0L, 0L, 0L, "", "Delhi");
        buildDeltaAlertLine(actualAlertText, delta);

        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");
    }

    @Test
    void deltaAlertLineMultipleTypes() {
        String expectedAlertText = "15 new cases, 9 recoveries in Maharashtra\n";
        AtomicReference<String> actualAlertText = new AtomicReference<>("");
        StatewiseDelta delta = new StatewiseDelta(9L, 0L, 15L, 0L, 0L, 0L, "", "Maharashtra");
        buildDeltaAlertLine(actualAlertText, delta);
        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");

        expectedAlertText = "15 new cases, 4 deaths in Delhi\n";
        actualAlertText = new AtomicReference<>("");
        delta = new StatewiseDelta(0L, 4L, 15L, 0L, 0L, 0L, "", "Delhi");
        buildDeltaAlertLine(actualAlertText, delta);
        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");

        expectedAlertText = "3 deaths, 9 recoveries in Delhi\n";
        actualAlertText = new AtomicReference<>("");
        delta = new StatewiseDelta(9L, 3L, 0L, 0L, 0L, 0L, "", "Delhi");
        buildDeltaAlertLine(actualAlertText, delta);
        assertEquals(expectedAlertText, actualAlertText.get(), "Alert text is not structured correctly!");
    }

    @Test
    void summaryAlertBlock() {
        final String expectedSummaryBlock = "\n<b>Total</b>\n" +
                "<pre>\n" +
                "Total cases  : (↑15) 5341\n" +
                "Active       : (↑2) 4729\n" +
                "Recovered    : (↑9) 455\n" +
                "Deaths       : (↑4) 157\n" +
                "Doubling rate: 250 days\n" +
                "</pre>";
        AtomicReference<String> actualSummaryBlock = new AtomicReference<>("");

        List<StatewiseDelta> deltas = Collections.singletonList(new StatewiseDelta(9L, 4L, 15L, 455L, 157L, 5341L, "", "Total"));
        List<StatewiseDelta> dailies = Collections.singletonList(new StatewiseDelta(9L, 4L, 15L, 0L, 0L, 0L, "", "Total"));
        Map<String, String> doublingRates = new HashMap<>();
        doublingRates.put("Total", "250");
        buildSummaryAlertBlock(actualSummaryBlock, deltas, dailies, emptyMap(), doublingRates, emptyMap());

        assertEquals(expectedSummaryBlock, actualSummaryBlock.get(), "Summary block is not structured correctly!");
    }

    @Test
    void summaryAlertBlockWithTestingData() {
        final String expectedSummaryBlock = "\n<b>Delhi</b>\n" +
                "<pre>\n" +
                "Total cases  : (↑15) 5341\n" +
                "Active       : (↑2) 4729\n" +
                "Recovered    : (↑9) 455\n" +
                "Deaths       : (↑4) 157\n" +
                "Doubling rate: 250 days\n" +
                "</pre>" +
                "<pre>" +
                "Total tested   : (↑19462) 53166\n" +
                "Positive       : (↑38) 1621\n" +
                "Negative       : 51161\n" +
                "Unconfirmed    : 384\n" +
                "Positivity rate: 3.05%\n" +
                "Last updated   : 26/04/2020\n" +
                "</pre>\n";
        AtomicReference<String> actualSummaryBlock = new AtomicReference<>("");

        List<StatewiseDelta> deltas = Collections.singletonList(new StatewiseDelta(9L, 4L, 15L, 455L, 157L, 5341L, "", "Delhi"));
        List<StatewiseDelta> dailies = Collections.singletonList(new StatewiseDelta(9L, 4L, 15L, 0L, 0L, 0L, "", "Delhi"));
        Map<String, String> doublingRates = new HashMap<>();
        doublingRates.put("Delhi", "250");
        Map<String, StatewiseTestData> testing = new HashMap<>();
        testing.put("Delhi", new StatewiseTestData("51161", "", "", "", "", "1621", "", "", "Delhi", "", "", "", "", "53166", "384", "26/04/2020", "19462", "38"));
        buildSummaryAlertBlock(actualSummaryBlock, deltas, dailies, testing, doublingRates, emptyMap());

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
                "Total cases  : (↑1) 28\n" +
                "Active       : (↑1) 28\n" +
                "Recovered    : (↑0) 0\n" +
                "Deaths       : (↑0) 0\n" +
                "Doubling rate: 19.44 days\n" +
                "</pre>" +
                "\n" +
                "<b>Himachal Pradesh</b>\n" +
                "<pre>\n" +
                "Total cases  : (↑9) 27\n" +
                "Active       : (↑9) 24\n" +
                "Recovered    : (↑0) 1\n" +
                "Deaths       : (↑0) 2\n" +
                "Doubling rate: 2.10 days\n" +
                "</pre>" +
                "\n" +
                "<b>Total</b>\n" +
                "<pre>\n" +
                "Total cases  : (↑31) 5341\n" +
                "Active       : (↑20) 4729\n" +
                "Recovered    : (↑8) 455\n" +
                "Deaths       : (↑3) 157\n" +
                "Doubling rate: 116 days\n" +
                "</pre>";

        String lastUpdated = "April 08, 12:04 AM";

        List<StatewiseDelta> dailies = asList(
                new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 0L, "08/04/2020 23:41:35", "Assam"),
                new StatewiseDelta(0L, 0L, 9L, 0L, 0L, 0L, "08/04/2020 00:04:28", "Himachal Pradesh"),
                new StatewiseDelta(8L, 3L, 31L, 0L, 0L, 0L, "08/04/2020 00:04:28", "Total"));
        List<StatewiseDelta> deltas = asList(
                new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 28L, "08/04/2020 23:41:35", "Assam"),
                new StatewiseDelta(0L, 0L, 9L, 1L, 2L, 27L, "08/04/2020 00:04:28", "Himachal Pradesh"),
                new StatewiseDelta(0L, 0L, 9L, 455L, 157L, 5341L, "08/04/2020 00:04:28", "Total"));
        Map<String, String> doublingRates = new HashMap<>();
        doublingRates.put("Assam", "19.44");
        doublingRates.put("Himachal Pradesh", "2.10");
        doublingRates.put("Total", "116");


        final String actualFinalAlert = buildStatewiseAlertText(lastUpdated, deltas, dailies, emptyMap(), doublingRates, emptyList());

        assertEquals(expectedFinalAlert, actualFinalAlert, "Summary block is not structured correctly!");
    }

    @Test
    void testStatewiseAlertTextWithDistricts() {
        final String expectedFinalAlert = "<i>April 08, 12:04 AM</i>\n" +
                "\n" +
                "1 new case in Maharashtra\n\n" +
                "<b>District-wise breakup</b>\n" +
                "1 new case in Mumbai\n" +
                "\n" +
                "<b>Total</b>\n" +
                "<pre>\n" +
                "Total cases  : (↑31) 5341\n" +
                "Active       : (↑20) 4729\n" +
                "Recovered    : (↑8) 455\n" +
                "Deaths       : (↑3) 157\n" +
                "Doubling rate: 116 days\n" +
                "</pre>\n" +
                "<b>Maharashtra</b>\n" +
                "<pre>\n" +
                "Total cases  : (↑1) 28\n" +
                "Active       : (↑1) 28\n" +
                "Recovered    : (↑0) 0\n" +
                "Deaths       : (↑0) 0\n" +
                "Doubling rate: 19.44 days\n" +
                "</pre>";

        String lastUpdated = "April 08, 12:04 AM";

        List<StatewiseDelta> dailies = asList(
                new StatewiseDelta(8L, 3L, 31L, 0L, 0L, 0L, "08/04/2020 00:04:28", "Total"),
                new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 0L, "08/04/2020 23:41:35", "Maharashtra"));
        List<StatewiseDelta> deltas = asList(
                new StatewiseDelta(0L, 0L, 9L, 455L, 157L, 5341L, "08/04/2020 00:04:28", "Total"),
                new StatewiseDelta(0L, 0L, 1L, 0L, 0L, 28L, "08/04/2020 23:41:35", "Maharashtra"));
        List<DistrictwiseData> districtDeltas = asList(
                new DistrictwiseData("Maharashtra", "Mumbai", "28", "28", "0", "0", "1", "0", "0", ""),
                new DistrictwiseData("Maharashtra", "Nagpur", "17", "13", "0", "2", "0", "0", "0", "")); // should be ignored as irrelevant
        Map<String, String> doublingRates = new HashMap<>();
        doublingRates.put("Total", "116");
        doublingRates.put("Maharashtra", "19.44");


        final String actualFinalAlert = buildStatewiseAlertText(lastUpdated, deltas, dailies, emptyMap(), doublingRates, districtDeltas);

        assertEquals(expectedFinalAlert, actualFinalAlert, "Summary block is not structured correctly!");
    }
}
