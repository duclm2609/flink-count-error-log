import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class Application {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Set up the Kafka source
        KafkaSource<ObjectNode> kafkaSource = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers("localhost:9093")
                .setTopics("error-log")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JSONDeserializationSchema())
                .build();

        // Add the Kafka source to the environment
        DataStream<ObjectNode> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy
                .<ObjectNode>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<ObjectNode>() {
                    @Override
                    public long extractTimestamp(ObjectNode event, long recordTimestamp) {
                        String timestampStr = event.get("@timestamp").asText();
                        Instant instant = Instant.parse(timestampStr);
                        return instant.toEpochMilli();
                    }
                }), "Kafka Source");

        DataStream<Tuple4<Instant, String, String, Long>> resultStream = kafkaStream
                .keyBy(new KeySelector<ObjectNode, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(ObjectNode node) throws Exception {
                        String scrumteam = "";
                        String service = "";
                        try {
                            JsonNode orgs = node.get("organization.name");
                            if (orgs != null) {
                                scrumteam = orgs.asText();
                            } else {
                                scrumteam = node.get("organization").get("name").asText();
                            }
                            JsonNode s = node.get("service.name");
                            if (s != null) {
                                service = s.asText();
                            } else {
                                service = s.get("service").get("name").asText();
                            }
                        } catch (Exception e) {
                            LOG.warn("Failed to parse JSON: {}", node, e);
                            return Tuple2.of("error", "error");
                        }
                        return Tuple2.of(scrumteam, service);
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .process(new ProcessWindowFunction<ObjectNode, Tuple4<Instant, String, String, Long>, Tuple2<String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<String, String> key, Context context, Iterable<ObjectNode> elements, Collector<Tuple4<Instant, String, String, Long>> out) {
                        long count = 0;
                        for (ObjectNode element : elements) {
                            count++;
                        }
                        out.collect(new Tuple4<>(Instant.now(), key.f0, key.f1, count));
                    }
                });

        resultStream.addSink(new InfluxDbSink("9NVJcYtqCqnxl_HgcWStMA_QxzoMcIrGnPi88L_BBw9lQ5hgR0ADqBW0Abp3B57_N-hvAXG-XqfJw6mJgYXMCQ==", "error-log", "TCBS"));

        // Execute the Flink job
        env.execute("Flink Kafka JSON Processor");
    }

    public static class JSONDeserializationSchema implements DeserializationSchema<ObjectNode> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public ObjectNode deserialize(byte[] message) throws IOException {
            return (ObjectNode) objectMapper.readTree(message);
        }

        @Override
        public boolean isEndOfStream(ObjectNode nextElement) {
            return false;
        }

        @Override
        public TypeInformation<ObjectNode> getProducedType() {
            return TypeInformation.of(ObjectNode.class);
        }
    }
}