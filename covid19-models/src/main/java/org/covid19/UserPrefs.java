package org.covid19;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserPrefs {
    private String userId;

    private List<String> myStates; // arraylist to allow multiple choices in future

    // kafka compaction tombstone removal takes time (default 1d after message moves
    // out of dirty log). While this setting can be modified it is still not guaranteed
    // how soon message will be removed. To avoid annoying the user by sending alerts
    // after they have unsubscribed, we instead toggle this boolean flag
    private boolean isSubscribed;
}
