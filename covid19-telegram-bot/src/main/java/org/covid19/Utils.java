package org.covid19;

import org.apache.commons.lang3.tuple.Pair;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

public class Utils {
    static <A, B> List<Pair<A, B>> zip(List<A> listA, List<B> listB) {
        if (listA.size() != listB.size()) {
            throw new IllegalArgumentException("Lists must have same size");
        }

        List<Pair<A, B>> pairList = new LinkedList<>();

        for (int index = 0; index < listA.size(); index++) {
            pairList.add(Pair.of(listA.get(index), listB.get(index)));
        }
        return pairList;
    }

    static String friendlyTime(String lastUpdated) {
        final LocalDateTime localDateTime = LocalDateTime.parse(lastUpdated, DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
        return localDateTime.format(DateTimeFormatter.ofPattern("MMMM dd, hh:mm a"));
    }
}
