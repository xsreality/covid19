package org.covid19;

import com.google.gson.annotations.SerializedName;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
public class Message {
    @SerializedName("created_at") private String createdAt;
    @SerializedName("username") private String username;
    @SerializedName("user_id") private String userId;
    @SerializedName("message_id") private String messageId;
    private Boolean truncated;
}
