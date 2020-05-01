package org.covid19.district;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

import static java.lang.Long.parseLong;

@Slf4j
public class DistrictAlertUtils {
    public static String buildDistrictwiseAlert(List<DistrictwiseData> deltas, List<DistrictwiseData> dailies) {
        AtomicReference<String> alertText = new AtomicReference<>("");
        deltas.forEach(delta -> buildDistrictDeltaAlertLine(alertText, delta));
        if (alertText.get().isEmpty() || "\n".equalsIgnoreCase(alertText.get())) {
            LOG.info("No useful update to alert on. Skipping...");
            return "";
        }
//        buildDistrictSummaryAlertBlock(alertText, deltas, dailies);
        LOG.info("Districtwise alert text generated:\n{}", alertText.get());
        return alertText.get();
    }

    public static void buildDistrictDeltaAlertLine(AtomicReference<String> updateText, DistrictwiseData delta) {
        boolean confirmed = false, deaths = false, include = false;
        String textLine = "";
        if (parseLong(delta.getDeltaConfirmed()) > 0L) {
            include = true;
            confirmed = true;
            textLine = textLine.concat(String.format("%s new %s",
                    delta.getDeltaConfirmed(),
                    parseLong(delta.getDeltaConfirmed()) == 1L ? "case" : "cases"));
        }
        if (parseLong(delta.getDeltaDeceased()) > 0L) {
            deaths = true;
            include = true;
            textLine = textLine.concat(String.format("%s%s %s",
                    confirmed ? ", " : "",
                    delta.getDeltaDeceased(),
                    parseLong(delta.getDeltaDeceased()) == 1L ? "death" : "deaths"));
        }
        if (parseLong(delta.getDeltaRecovered()) > 0L) {
            include = true;
            textLine = textLine.concat(String.format("%s%s %s",
                    confirmed || deaths ? ", " : "",
                    delta.getDeltaRecovered(),
                    parseLong(delta.getDeltaRecovered()) == 1L ? "recovery" : "recoveries"));
        }
        if (include) {
            textLine = textLine.concat(String.format(" in %s\n", delta.getDistrict()));
        }
        updateText.accumulateAndGet(textLine, (current, update) -> current + update);

    }
}
