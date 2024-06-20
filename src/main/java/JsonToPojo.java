import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToPojo implements MapFunction<String, ErrorMessage> {
    private transient ObjectMapper mapper;

    @Override
    public ErrorMessage map(String value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return mapper.readValue(value, ErrorMessage.class);
    }
}
