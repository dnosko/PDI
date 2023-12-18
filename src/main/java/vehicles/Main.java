package vehicles;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
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
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class Main {
    public static final int CHECKPOINTING_INTERVAL_MS = 5000;
    private static final String JOB_NAME = "Streaming Argis data";
    public static void main(String[] args) throws Exception {
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // toto je pre lokalne spustenie a debugovanie.
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // todo pridat if ci test ak test tak to ide zo suboru alebo z iteratoru from Collection
        //  inak z websocketu?2

        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        DataStreamSource<String> mySocketStream = env.addSource(new MyWebSocketSourceFunc());
        DataStream<JsonNode> jsonStream = mySocketStream.map(jsonString -> mapToJson(jsonString));
        DataStream<Vehicle> vehicleStream = jsonStream.map(s -> mapToVehicle(s)).filter(new IsActiveFilter());
        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
        env.setRestartStrategy(RestartStrategies.noRestart());
        //vehicleStream.print();


        /* Vehicles going North */ //TODO potom urobit na to nejaku mozno factory? alebo nieco nech to je pekne podla prepinacov...
        // TODO upravit JSON format na nejaky krajsi po riadkoch?
       DataStream<Vehicle> northStream = vehicleStream.filter(new goingNorth());
        //northStream.print();
        final Path outputNorth = new Path("tmp/north");
        final DefaultRollingPolicy<Vehicle, String> rollingPolicy = DefaultRollingPolicy
                .builder()
                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                .withRolloverInterval(Duration.ofSeconds(10))
                .build();
        final FileSink<Vehicle> northSink = FileSink
                .<Vehicle>forRowFormat(outputNorth, new SimpleStringEncoder<>())
                .withRollingPolicy(rollingPolicy)
                .build();

        northStream.sinkTo(northSink).name("north-sink");

        /* Trains with last stops */

        DataStream<Vehicle> trainsStream = vehicleStream.filter(new FilterFunction<Vehicle>() {
            @Override
            public boolean filter(Vehicle v) throws Exception {
                return v.ltype == 5;
            }
        });

        trainsStream.keyBy(value -> value.id).flatMap(new LastStops());

       SingleOutputStreamOperator<String> printStream =
                trainsStream.map(new MapFunction<Vehicle, String>() {
            @Override
            public String map(Vehicle v) throws Exception {
                return "trainID:"+ v.id + " trainName:" + v.linename + " last stop:" +v.laststopid + " last update:"+ v.lastupdate;
            }
        });//.print();*/


        final Path outputAllTrainsLastStop = new Path("tmp/lastStop");
        final DefaultRollingPolicy<String, String> rollingPolicyLastStop = DefaultRollingPolicy
                .builder()
                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                .withRolloverInterval(Duration.ofSeconds(60))
                .build();
        final FileSink<String> lastStopSink = FileSink
                .<String>forRowFormat(outputAllTrainsLastStop, new SimpleStringEncoder<>())
                .withRollingPolicy(rollingPolicyLastStop)
                .build();

        printStream.sinkTo(lastStopSink).name("laststop-sink");
        /**************************************************************************************/
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //TODO GLOBALNE meskanie vypisovat seznam nejvýše 5 zpožděných vozů seřazených sestupně podle jejich posledně hlášeného zpoždění od startu aplikace

        /*DataStream<Vehicle> delayedVehicles =
                vehicleStream.filter(v -> v.delay > 0.0)
                .keyBy(v -> v.vtype)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                        .process(new WindowFunction()).map(new MapFunction<Vehicle, String>() {
            @Override
            public String map(Vehicle v) throws Exception {
                return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
            }
        }).print().setParallelism(1);*/

        //delayedVehicles.print();

        /* Top 5 delayed in 3 minutes window TODO nefunguje radenie a ma to byt bez allWindows iba windows :)))))) mozno pridat tie watermarks */
        DataStream<Vehicle> delayedStream = vehicleStream.filter(v -> v.delay > 0.0).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Vehicle>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new WindowDelayed())
                .map((MapFunction<Vehicle, String>) v -> "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate)
                .print()
                .setParallelism(1);
        /*************************************/
        /* Average for 3 minutes across all vehicles */
        DataStream<Tuple2<String, Double>> keyDelayStream = vehicleStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Vehicle>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .map(v -> new Tuple2<>(v.id, v.getDelay()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
        keyDelayStream.windowAll(TumblingEventTimeWindows.of(Time.minutes(3))).aggregate(new AverageAggregate())
                .print();


        env.execute(JOB_NAME);

    }

    private static class AverageWindowFunction
            extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

        public void process(String key,
                            Context context,
                            Iterable<Double> averages,
                            Collector<Tuple2<String, Double>> out) {
            Double average = averages.iterator().next();
            out.collect(new Tuple2<>(key, average));
        }
    }

    public static class ProcessAggregationWindow implements WindowFunction<Tuple2<String, Double>, Double, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Double>> values, Collector<Double> out) throws Exception {
            double sum = 0.0;
            int count = 0;

            for (Tuple2<String, Double> value : values) {
                sum += value.f1;
                count ++;
            }

            double avg = sum/count;
            out.collect(avg);
        }
    }

    public static class WindowDelayed extends ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow>  {

        @Override
        public void process(ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow>.Context context, Iterable<Vehicle> v, Collector<Vehicle> out) throws Exception {
            List<Vehicle> vehicles = new ArrayList<>();
            int topN = 5;
            for (Vehicle vehicle : v) {
                if (vehicle != null)
                    vehicles.add(vehicle);
            }

            // Sort the vehicles by delay
            vehicles.sort(Comparator.comparingDouble(Vehicle::getDelay).reversed());

            int topCount = Math.min(vehicles.size(), topN);
            List<Vehicle> top5Vehicles = vehicles.subList(0, topCount);
            top5Vehicles.sort(Comparator.comparingLong(Vehicle::getLastUpdateLong));
            //System.out.println("i am here" + topCount + " " + top5Vehicles.size());
            for (Vehicle ve: top5Vehicles) {
                out.collect(ve);
            }
        }

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

    private static Vehicle mapToVehicle(JsonNode jsonString) {
        try {
            return new Vehicle(jsonString.path("attributes"));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /*Filter only active vehicles*/
    private static class IsActiveFilter implements FilterFunction<Vehicle> {
        @Override
        public boolean filter(Vehicle v) throws Exception {
            String inactive = v.isinactive;
            return inactive.equals("false");
        }
    }

    /* Filter vehicles going to the North. Angle is specified by bearing attribute and 0 degrees is North.
       Deviation can be 45 degrees. Therefor 0-45 or 315-360 degrees */
    private static class goingNorth implements FilterFunction<Vehicle> {
        @Override
        public boolean filter(Vehicle v) throws Exception {
            double lowerBound = 0;
            double upperBound = 45;
            double upperBound2 = 315;
            return (v.bearing >= lowerBound && v.bearing <= upperBound) || v.bearing >= upperBound2;
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

