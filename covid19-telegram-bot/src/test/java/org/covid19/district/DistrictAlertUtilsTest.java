package org.covid19.district;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.covid19.district.DistrictAlertUtils.buildDistrictwiseAlert;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DistrictAlertUtilsTest {
    @Test
    public void testBuildDistrictwiseAlert() {
        List<DistrictwiseData> deltas = asList(
                new DistrictwiseData("Maharashtra", "Mumbai", "120", "90", "5", "9", "12", "3", "1", "")
        );
        List<DistrictwiseData> dailies = asList(
                new DistrictwiseData("Maharashtra", "Mumbai", "25", "40", "12", "3", "11", "23", "4", "")
        );
        AtomicReference<String> alertText = new AtomicReference<>();

        String expected = "12 new cases, 1 death, 3 recoveries in Mumbai\n";
        String actual = buildDistrictwiseAlert(deltas, dailies);

        assertEquals(expected, actual);
    }
}
