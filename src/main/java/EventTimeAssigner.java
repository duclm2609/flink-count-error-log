import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

import java.time.Instant;

public class EventTimeAssigner implements TimestampAssigner<JsonNode> {
    @Override
    public long extractTimestamp(JsonNode element, long recordTimestamp) {
        String timestampStr = element.get("@timestamp").asText();
        Instant instant = Instant.parse(timestampStr);
        return instant.toEpochMilli();
    }
}