package org.covid19;

import org.apache.commons.lang3.tuple.Pair;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {
    public static <A, B> List<Pair<A, B>> zip(List<A> listA, List<B> listB) {
        if (listA.size() != listB.size()) {
            throw new IllegalArgumentException("Lists must have same size");
        }

        List<Pair<A, B>> pairList = new LinkedList<>();

        for (int index = 0; index < listA.size(); index++) {
            pairList.add(Pair.of(listA.get(index), listB.get(index)));
        }
        return pairList;
    }

    public static String friendlyTime(String lastUpdated) {
        try {
            final LocalDateTime localDateTime = LocalDateTime.parse(lastUpdated, DateTimeFormatter.ofPattern("d/MM/yyyy HH:mm:ss"));
            return localDateTime.format(DateTimeFormatter.ofPattern("MMMM dd, hh:mm a"));
        } catch (DateTimeParseException e) {
            LOG.warn("Error parsing date '{}' with error {}", lastUpdated, e);
            return null;
        }
    }

    public static Map<String, String> initStateCodes() {
        Map<String, String> stateCodes = new HashMap<>();
        stateCodes.put("Total", "Total");
        stateCodes.put("Andhra Pradesh", "AP");
        stateCodes.put("Arunachal Pradesh", "AR");
        stateCodes.put("Assam", "Assam");
        stateCodes.put("Bihar", "Bihar");
        stateCodes.put("Chhattisgarh", "CT");
        stateCodes.put("Goa", "Goa");
        stateCodes.put("Gujarat", "Guja");
        stateCodes.put("Haryana", "HR");
        stateCodes.put("Himachal Pradesh", "HP");
        stateCodes.put("Jharkhand", "JH");
        stateCodes.put("Karnataka", "KA");
        stateCodes.put("Kerala", "Ker");
        stateCodes.put("Madhya Pradesh", "MP");
        stateCodes.put("Maharashtra", "Mah");
        stateCodes.put("Manipur", "Mani");
        stateCodes.put("Meghalaya", "Megh");
        stateCodes.put("Mizoram", "Mizo");
        stateCodes.put("Nagaland", "Naga");
        stateCodes.put("Odisha", "Odis");
        stateCodes.put("Punjab", "Punj");
        stateCodes.put("Rajasthan", "Raj");
        stateCodes.put("Sikkim", "Sikk");
        stateCodes.put("Tamil Nadu", "TN");
        stateCodes.put("Telangana", "Telg");
        stateCodes.put("Tripura", "Trip");
        stateCodes.put("Uttarakhand", "UT");
        stateCodes.put("Uttar Pradesh", "UP");
        stateCodes.put("West Bengal", "WB");
        stateCodes.put("Andaman and Nicobar Islands", "A&N");
        stateCodes.put("Chandigarh", "CH");
        stateCodes.put("Dadra and Nagar Haveli", "DNH");
        stateCodes.put("Daman and Diu", "DD");
        stateCodes.put("Delhi", "Delhi");
        stateCodes.put("Jammu and Kashmir", "J&K");
        stateCodes.put("Ladakh", "LDK");
        stateCodes.put("Lakshadweep", "LDWP");
        stateCodes.put("Puducherry", "Pudu");
        stateCodes.put("State Unassigned", "Unass");
        return stateCodes;
    }

}
