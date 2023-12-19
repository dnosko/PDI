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


        /* Vehicles going North */
        //TODO potom urobit na to nejaku mozno factory? alebo nieco nech to je pekne podla prepinacov...
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

        /* Trains with last stops
        * Nie som si ista ci to funguje uplne spravne hh ten flatmap ci nema byt tiez cez nejake globalne okno napr. */

       SingleOutputStreamOperator<String> printTrainLastStop =  trainLastStop(vehicleStream);//.print();*/


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

        printTrainLastStop.sinkTo(lastStopSink).name("laststop-sink");
        /**************************************************************************************/
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        /* top delayed vehicles since start of application, sorting doesnt work...*/
        //mostDelayedVehicles(vehicleStream).print().setParallelism(1);


        /* top 5 delayed vehicles in 3 minutes window, sorting doesnt work..*/
        int windowMinutes = 1;
        mostDelayedVehiclesInWindow(vehicleStream, windowMinutes).print().setParallelism(1);

        /* Average for 3 minutes across all vehicles */
        int delayWindowInMinutes = 3;
        averageDelay(vehicleStream, delayWindowInMinutes);

        env.execute(JOB_NAME);

    }


    /* Counts average delay across all vehicles in time window of N minutes*/
    private static SingleOutputStreamOperator<Double> averageDelay(DataStream<Vehicle> vehicleStream, int minutes){
        DataStream<Tuple2<String, Double>> keyDelayStream = vehicleStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Vehicle>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .map(v -> new Tuple2<>(v.id, v.getDelay()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));

        return keyDelayStream.windowAll(TumblingEventTimeWindows.of(Time.minutes(minutes))).aggregate(new AverageAggregate());
    }

    /* Most delayed vehicles in time window specified by minutes
     * TODO sorting doesnt work yet
     * */
    private static SingleOutputStreamOperator<String> mostDelayedVehiclesInWindow(DataStream<Vehicle> vehicleStream, int minutes){
        DataStream<Vehicle> delayedStream = vehicleStream.filter(v -> v.delay > 0.0).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Vehicle>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()));

        return delayedStream.windowAll(TumblingProcessingTimeWindows.of(Time.minutes(minutes)))
                .process(new MostDelayedInWindow())
                .map(new MapFunction<Vehicle, String>() {
                    @Override
                    public String map(Vehicle v) throws Exception {
                        return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
                    }
                });
    }

    /* Outputs stream of most delayed vehicles since start of application, outputing resulsts every 10 seconds. .
    * TODO sorting doesnt work yet
    * */
    private static SingleOutputStreamOperator<String> mostDelayedVehicles(DataStream<Vehicle> vehicleStream){
        DataStream<Vehicle> delayedStream = vehicleStream.filter(v -> v.delay > 0.0).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Vehicle>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()));

        return delayedStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new MostDelayedGlobally())
                .map(new MapFunction<Vehicle, String>() {
                    @Override
                    public String map(Vehicle v) throws Exception {
                        return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
                    }
                });
    }

    /* Converts input stream into stream filtering only trains and showing their last stops */
    private static SingleOutputStreamOperator<String> trainLastStop(DataStream<Vehicle> vehicleStream){
        DataStream<Vehicle> trainsStream = vehicleStream.filter(new FilterFunction<Vehicle>() {
            @Override
            public boolean filter(Vehicle v) throws Exception {
                return v.ltype == 5;
            }
        });

        trainsStream.keyBy(value -> value.id).flatMap(new LastStops());

       return trainsStream.map(new MapFunction<Vehicle, String>() {
                    @Override
                    public String map(Vehicle v) throws Exception {
                        return "trainID:"+ v.id + " trainName:" + v.linename + " last stop:" +v.laststopid + " last update:"+ v.lastupdate;
                    }
                });
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

