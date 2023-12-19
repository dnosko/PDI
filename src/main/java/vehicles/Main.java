package vehicles;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import lombok.extern.slf4j.Slf4j;
import java.time.Duration;
import java.util.*;


/**
 * TODO:
 * pridat prepinace
 * urobit do funkcii jednotlive tasky
 * urobit triedy pre tie specialne funkcie a nemat to tu pokope v jednom
 * prehodit websocket do vlasnej triedy
 * urobit testy
 * **/
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

        DataStreamSource<String> mySocketStream = env.addSource(new WebSocketStream());
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

        /* top 5 delayed vehicles of all time, sorting doesnt work...*/
        DataStream<Vehicle> delayedStream = vehicleStream.filter(v -> v.delay > 0.0).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Vehicle>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()));

        delayedStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new MostDelayedProcessAllWindowFunction())
            .map(new MapFunction<Vehicle, String>() {
            @Override
            public String map(Vehicle v) throws Exception {
                return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
            }
        }).print().setParallelism(1);

        //delayedVehicles.print();

        /* Top 5 delayed in 3 minutes window
        TODO nefunguje radenie a ma to byt allWindows lebo nechcem grupovat cez kluce a  skusit urobit cez aggregate ako average??? */
        /*DataStream<Vehicle> delayedStream = vehicleStream.filter(v -> v.delay > 0.0).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Vehicle>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .flatMap(new AllTimeDelayed()).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .map((MapFunction<Vehicle, String>) v -> "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate)
                .print()
                .setParallelism(1);*/
        /*************************************/

        /* Average for 3 minutes across all vehicles */
        DataStream<Tuple2<String, Double>> keyDelayStream = vehicleStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Vehicle>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .map(v -> new Tuple2<>(v.id, v.getDelay()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
        keyDelayStream.windowAll(TumblingEventTimeWindows.of(Time.minutes(3))).aggregate(new AverageAggregate());
                //.print();

        env.execute(JOB_NAME);

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



}

