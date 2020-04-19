package org.covid19;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static java.time.ZoneId.of;
import static java.time.format.DateTimeFormatter.ofPattern;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UtilTests {
    @Test
    public void isRecentDateTest() {
        LocalDate today = LocalDate.now(of("Asia/Kolkata"));
        LocalDate yesterday = today.minusDays(6L);
        String yesterdayStr = yesterday.format(ofPattern("dd/MM/yyyy"));

        assertTrue(Covid19PatientAnalyzer.isWithinPastWeek(yesterdayStr));

    }
}
