import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.Instant;

public class InfluxDbSink extends RichSinkFunction<Tuple4<Instant, String, String, Long>> {

    private transient InfluxDBClient influxDBClient;
    private String token;
    private String bucket;
    private String org;

    public InfluxDbSink(String token, String bucket, String org) {
        this.token = token;
        this.bucket = bucket;
        this.org = org;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());
    }

    @Override
    public void invoke(Tuple4<Instant, String, String, Long> value, Context context) throws Exception {
        Point point = Point.measurement("events")
                .addTag("scrum_team", value.f1)
                .addTag("service_name", value.f2)
                .addField("count", value.f3)
                .time(value.f0, WritePrecision.MS);

        influxDBClient.getWriteApiBlocking().writePoint(bucket, org, point);
    }

    @Override
    public void close() throws Exception {
        if (influxDBClient != null) {
            influxDBClient.close();
        }
    }
}
