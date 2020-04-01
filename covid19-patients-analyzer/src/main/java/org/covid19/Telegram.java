package org.covid19;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@Data
public class Telegram {
    private String username;
    @JsonProperty("user_id") private String userId;
    @JsonProperty("message_id") private String messageId;
}
