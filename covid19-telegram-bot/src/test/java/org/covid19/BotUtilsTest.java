package org.covid19;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.covid19.bot.BotUtils.buildDistrictZoneText;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BotUtilsTest {
    @Test
    public void zonesText() {
        Map<String, String> data = new HashMap<>();
        data.put("Mumbai", "Green");
        data.put("Pune", "Orange");
        data.put("Nagpur", "Red");
        data.put("Chandrapur", "Unknown");

        String state = "Maharashtra";

        String expected = "<b>Districts of Maharashtra</b>\n" +
                "\n" +
                "\u2753   Chandrapur\n" +
                "\uD83D\uDD34   Nagpur\n" +
                "\uD83D\uDD36   Pune\n" +
                "\uD83D\uDC9A   Mumbai\n" +
                "\n" +
                "https://twitter.com/pib_india/status/1256468081896878080";

        String actual = buildDistrictZoneText(state, data);
        assertEquals(expected, actual);
    }
}
