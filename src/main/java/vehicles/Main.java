package vehicles;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import lombok.extern.slf4j.Slf4j;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class Main {
    public static final int CHECKPOINTING_INTERVAL_MS = 5000;
    private static final String JOB_NAME = "Streaming Argis data";
    public static void main(String[] args) throws Exception {
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // toto je pre lokalne spustenie a debugovanie.
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // todo pridat if ci test ak test tak to ide zo suboru alebo z iteratoru from Collection
        //  inak z websocketu?2

        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        DataStreamSource<String> mySocketStream = env.addSource(new MyWebSocketSourceFunc());
        DataStream<JsonNode> vehicleStream = mySocketStream.map(jsonString -> mapToJson(jsonString)).filter(new IsActiveFilter());
        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
        env.setRestartStrategy(RestartStrategies.noRestart());


        /* Vehicles going North */ //TODO potom urobit na to nejaku mozno factory? alebo nieco nech to je pekne podla prepinacov...
        // TODO upravit JSON format na nejaky krajsi po riadkoch?
        DataStream<JsonNode> northStream = vehicleStream.filter(new goingNorth());
        northStream.print();
        final Path outputNorth = new Path("tmp/north");
        final DefaultRollingPolicy<JsonNode, String> rollingPolicy = DefaultRollingPolicy
                .builder()
                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                .withRolloverInterval(Duration.ofSeconds(10))
                .build();
        final FileSink<JsonNode> northSink = FileSink
                .<JsonNode>forRowFormat(outputNorth, new SimpleStringEncoder<>())
                .withRollingPolicy(rollingPolicy)
                .build();

        northStream.sinkTo(northSink).name("north-sink");


        env.execute(JOB_NAME);

    }
    private static JsonNode mapToJson(String jsonString) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readTree(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /*Filter only active vehicles*/
    private static class IsActiveFilter implements FilterFunction<JsonNode> {
        @Override
        public boolean filter(JsonNode jsonNode) throws Exception {
            return jsonNode.path("attributes").path("isinactive").asText().equals("false");
        }
    }

    /* Filter vehicles going to the North. Angle is specified by bearing attribute and 0 degrees is North.
       Deviation can be 45 degrees. Therefor 0-45 or 315-360 degrees */
    private static class goingNorth implements FilterFunction<JsonNode> {
        @Override
        public boolean filter(JsonNode jsonNode) throws Exception {
            double bearing = jsonNode.path("attributes").path("bearing").asDouble();
            double lowerBound = 0;
            double upperBound = 45;
            double upperBound2 = 315;
            return (bearing >= lowerBound && bearing <= upperBound) || bearing >= upperBound2;
        }
        }


    // from https://gist.github.com/tonvanbart/17dc93be413f7c53b76567e10b87a141
    public static class MyWebSocketSourceFunc extends RichSourceFunction<String> {
        private boolean running = true;
        transient AsyncHttpClient client;
        transient BoundRequestBuilder boundRequestBuilder;
        transient WebSocketUpgradeHandler.Builder webSocketListener;
        private BlockingQueue<String> messages = new ArrayBlockingQueue<>(100);

        private final String websocketURI = "wss://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326";

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            WebSocketUpgradeHandler webSocketUpgradeHandler = webSocketListener.addWebSocketListener(
                    new WebSocketListener() {

                        private final ObjectMapper myMapper = new ObjectMapper();

                        private String getRsvpId(String payload) {
                            try {
                                Map map = myMapper.readValue(payload, Map.class);
                                Object rsvpId = map.get("rsvp_id");
                                return rsvpId != null ? rsvpId.toString() : "NOT FOUND";
                            } catch (IOException e) {
                                log.error("Mapping failed, returning 'null'");
                                return "NULL";
                            }
                        }

                        @Override
                        public void onOpen(WebSocket webSocket) {
                        }

                        @Override
                        public void onClose(WebSocket webSocket, int i, String s) {
                        }

                        @Override
                        public void onError(Throwable throwable) {
                        }

                        @Override
                        public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                            log.debug("onTextFrame({}), rsvp_id={}", hash(payload), getRsvpId(payload));
                            //System.out.println(payload);
                            if (payload != null) {
                                try {
                                    messages.put(payload);
                                } catch (InterruptedException e) {
                                    log.error("Interrupted!", e);
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    }).build();
            boundRequestBuilder.execute(webSocketUpgradeHandler).get();

            while (running) {
                ctx.collect(messages.take());
            }
            running = false;
        }

        @Override
        public void cancel() {
            log.info("cancel function called");
            running = false;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            log.info("open function called");
            super.open(parameters);
            client = Dsl.asyncHttpClient();
            boundRequestBuilder = client.prepareGet(websocketURI);
            webSocketListener = new WebSocketUpgradeHandler.Builder();
        }

        private String hash(String input) {
            if (input == null) {
                return "-- NULL --";
            }

            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(input.getBytes());
                byte[] digest = md.digest();
                return DatatypeConverter.printHexBinary(digest).toUpperCase();
            } catch (NoSuchAlgorithmException e) {
                log.error("Cound not instantiate MD5", e);
                return "--NOT CALCULATED--";
            }
        }
    }

    public static class MapIt extends RichMapFunction<String, String> {

        final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public String map(String value) throws Exception {
            Map<String, Object> mapped = objectMapper.readValue(value, Map.class);
            Object rsvp = mapped.get("rsvp_id");
            return rsvp != null ? rsvp.toString() : "null" ;
        }
    }
}

