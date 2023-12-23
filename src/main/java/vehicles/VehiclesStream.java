package vehicles;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;


/**
 * TODO:
 * pridat prepinace
 * urobit testy
 * **/
@Slf4j
public class VehiclesStream {
    public static final int CHECKPOINTING_INTERVAL_MS = 5000;
    private static final String JOB_NAME = "Streaming Argis data";
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
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

        DataStream<String> outputStream = vehicleStream.map(Vehicle::toString);


        if (parameters.has("h") || (parameters.has("help"))) {
            printHelp();
            System.exit(0); // Terminate the program after printing help
        }
        else if (parameters.has("1") || (parameters.has("north"))) {
            outputStream = vehiclesGoingNorth(vehicleStream).map(Vehicle::toString);
        }
        else if (parameters.has("2") || (parameters.has("trains"))) {
            outputStream = trainLastStop(vehicleStream).map(new MapFunction<Vehicle, String>() {
                @Override
                public String map(Vehicle v) throws Exception {
                    return "trainID:"+ v.id + " trainName:" + v.linename + " last stop:" +v.laststopid + " last update:"+ v.lastupdate;
                }
            });
        }
        else if (parameters.has("3") || (parameters.has("delayed"))) {
            env.setParallelism(1);
            outputStream = mostDelayedVehicles(vehicleStream).map(new MapFunction<Vehicle, String>() {
                @Override
                public String map(Vehicle v) throws Exception {
                    return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
                }
            });
        }
        else if (parameters.has("4") || (parameters.has("delayedw"))) {
            int windowMinutes = 3;
            env.setParallelism(1);
            outputStream = mostDelayedVehiclesInWindow(vehicleStream, windowMinutes).map(new MapFunction<Vehicle, String>() {
                @Override
                public String map(Vehicle v) throws Exception {
                    return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
                }
            });

        }
        else if (parameters.has("5") || (parameters.has("average"))) {
            int delayWindowInMinutes = 1;
            outputStream = averageDelay(vehicleStream, delayWindowInMinutes).map(v -> "Average delay:" + v.toString());
        }
        else if (parameters.has("6") || (parameters.has("diff"))) {
            int sizeOfWindow = 10;
            outputStream = averageTimeBetweenRecords(vehicleStream, sizeOfWindow).map(new MapFunction<Tuple2<String, Double>,String>() {
                @Override
                public  String map(Tuple2<String,Double> result) throws Exception {
                    return "ID: " + result.f0 + " Average: " + result.f1;
                }
            });
        }

        if (parameters.has("output")) {
            final Path output = new Path(parameters.getRequired("output"));
            final DefaultRollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy
                    .builder()
                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                    .withRolloverInterval(Duration.ofSeconds(10))
                    .build();
            final FileSink<String> fileSink = FileSink
                    .<String>forRowFormat(output, new SimpleStringEncoder<>())
                    .withRollingPolicy(rollingPolicy)
                    .build();
            outputStream.sinkTo(fileSink).name("file-sink");
        }
        else {
            outputStream.print();
        }



        /* Vehicles going North */
        //TODO potom urobit na to nejaku mozno factory? alebo nieco nech to je pekne podla prepinacov...
        // TODO upravit JSON format na nejaky krajsi po riadkoch?
       /*DataStream<Vehicle> northStream = vehiclesGoingNorth(vehicleStream);
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
        */
        /* Trains with last stops
        * Nie som si ista ci to funguje uplne spravne hh ten flatmap ci nema byt tiez cez nejake globalne okno napr. */

       /*SingleOutputStreamOperator<String> printTrainLastStop =  trainLastStop(vehicleStream).map(new MapFunction<Vehicle, String>() {
            @Override
            public String map(Vehicle v) throws Exception {
                return "trainID:"+ v.id + " trainName:" + v.linename + " last stop:" +v.laststopid + " last update:"+ v.lastupdate;
            }
        });//.print();*/


        /*final Path outputAllTrainsLastStop = new Path("tmp/lastStop");
        final DefaultRollingPolicy<String, String> rollingPolicyLastStop = DefaultRollingPolicy
                .builder()
                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                .withRolloverInterval(Duration.ofSeconds(60))
                .build();
        final FileSink<String> lastStopSink = FileSink
                .<String>forRowFormat(outputAllTrainsLastStop, new SimpleStringEncoder<>())
                .withRollingPolicy(rollingPolicyLastStop)
                .build();

        printTrainLastStop.sinkTo(lastStopSink).name("laststop-sink");*/
        /**************************************************************************************/
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        /* top delayed vehicles since start of application, sorting doesnt work...*/
        /*mostDelayedVehicles(vehicleStream).map(new MapFunction<Vehicle, String>() {
            @Override
            public String map(Vehicle v) throws Exception {
                return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
            }
        }).print().setParallelism(1);


        /* top 5 delayed vehicles in 3 minutes window, sorting doesnt work..*/
        /*int windowMinutes = 1;
        mostDelayedVehiclesInWindow(vehicleStream, windowMinutes).map(new MapFunction<Vehicle, String>() {
                            @Override
                            public String map(Vehicle v) throws Exception {
                                return "ID:"+ v.id + " name:" + v.linename + " delay:" +v.delay + " last update:"+ v.lastupdate;
                            }
        }).print().setParallelism(1);*/

        /* Average for 3 minutes across all vehicles idk ci ok s tym stringom*/
        int delayWindowInMinutes = 1;
        //agverageDelay(vehicleStream, delayWindowInMinutes).map(v -> "Average delay:" + v.toString()).print();

        /* Average time between input data with 10 latest records for each vehicle */
       //averageTimeBetweenRecords(vehicleStream, 10).print();

        env.execute(JOB_NAME);

    }

    private static void printHelp() {
        System.out.println("Usage: FlinkJob [OPTIONS]");
        System.out.println(" If no options are specified, the job will just print the stream of data.");
        System.out.println("Options:");
        System.out.println("  -h, --help   Print this help message");
        System.out.println("  -1, --north   Stream only vehicles going north");
        System.out.println("  -2, --trains   Stream all trains since start of application with their last stop and last update");
        System.out.println("  -3, --delayed   Stream top 5 delayed vehicles since start of application ");
        System.out.println("  -4, --delayedw   Stream top 5 delayed vehicles in 3 minute windows");
        System.out.println("  -5, --average   Counts global average of delay in 3 minute windows");
        System.out.println("  -6, --diff   Counts average time between incoming records for last 10 records for each vehicle");
    }

    /* Calculates the average time between records for each vehicle within last N records */
    public static SingleOutputStreamOperator<Tuple2<String, Double>> averageTimeBetweenRecords(DataStream<Vehicle> vehicleStream, int numberOfRecords){
        return vehicleStream
                .keyBy(v -> v.id)
                .countWindow(numberOfRecords, 1)
                .aggregate(new AverageRecords());
    }

    /* Counts average delay across all vehicles in time window of N minutes*/
    public static SingleOutputStreamOperator<Double> averageDelay(DataStream<Vehicle> vehicleStream, int minutes){
        return vehicleStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Vehicle>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(minutes)))
                .aggregate(new AverageAggregate());
    }

    /* Most delayed vehicles in time window specified by minutes
     * TODO sorting works in tests takze skor vyriesit iba paralelizmus
     * */
    public static SingleOutputStreamOperator<Vehicle> mostDelayedVehiclesInWindow(DataStream<Vehicle> vehicleStream, int minutes){
       return vehicleStream
                .filter(v -> v.delay > 0.0)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Vehicle>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(minutes)))
                .process(new MostDelayedInWindow())
               ;
    }

    /* Outputs stream of most delayed vehicles since start of application, outputing resulsts every 10 seconds. .
    * TODO sorting doesnt work yet - asi nastavit iba paraleizmus? env.setParallelism(1)
    * */
    public static SingleOutputStreamOperator<Vehicle> mostDelayedVehicles(DataStream<Vehicle> vehicleStream){
        return vehicleStream.filter(v -> v.delay > 0.0)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Vehicle>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new MostDelayedGlobally());
    }

    /* Filter vehicles going in the north direction */
    public static DataStream<Vehicle> vehiclesGoingNorth(DataStream<Vehicle> vehicleStream){
        return vehicleStream.filter(new goingNorth());
    }

    /* Converts input stream into stream filtering only trains and showing their last stops */
    public static SingleOutputStreamOperator<Vehicle> trainLastStop(DataStream<Vehicle> vehicleStream){
        return vehicleStream
                .filter((FilterFunction<Vehicle>) v -> v.vtype == (short) 5)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Vehicle>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getLastUpdateLong()))
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new LastStops());
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

