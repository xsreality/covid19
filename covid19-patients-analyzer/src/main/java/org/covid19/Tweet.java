package org.covid19;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@Data
public class Tweet {
    @JsonProperty("created_at") private String createdAt;
    @JsonProperty("id_str") private String id;
    private boolean truncated;
    @JsonProperty("user_id") private String userId;
    @JsonProperty("screen_name") private String userScreenName;
    private String text;
    @JsonProperty("full_text") private String fullText;
}
