package org.covid19;

import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UserPrefsSerdeTest {

    @Test
    public void testUserPrefsSerde() {
        UserPrefs prefs = new UserPrefs("chatId", emptyList(), true);
        UserPrefsSerde userPrefsSerde = new UserPrefsSerde();
        final byte[] prefsBytes = userPrefsSerde.serializer().serialize("anytopic", prefs);

        final UserPrefs deserializedPrefs = userPrefsSerde.deserializer().deserialize("anytopic", prefsBytes);

        assertEquals(prefs, deserializedPrefs);
    }
}
