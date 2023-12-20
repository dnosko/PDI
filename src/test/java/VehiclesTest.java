package vehicles;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.apache.flink.api.common.functions.util.ListCollector;
import java.util.Date;
import org.apache.flink.api.common.RuntimeExecutionMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class VehiclesTest {
    Main main = new Main();

    @Test
    public void testVehiclesGoingNorth()  throws Exception {

        Vehicle A = new Vehicle("1", (short) 1, 340, 1, "L1", 0, 10, System.currentTimeMillis());
        Vehicle B = new Vehicle("2", (short) 1, 0, 2, "L2", 0, 20, System.currentTimeMillis() );
        Vehicle C = new Vehicle("3", (short) 1, 90, 1, "L1", 0, 80,System.currentTimeMillis() );
        Vehicle D = new Vehicle("4", (short) 1, 45, 4, "L3", 0, 15, System.currentTimeMillis()  );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CollectSink collector = new CollectSink();

        DataStream<Vehicle> vehicleStream = env.fromElements(A, B, C, D);
        main.vehiclesGoingNorth(vehicleStream).addSink(collector);

        env.execute();

        List<Vehicle> collectedResults = collector.getRecords();
        List<Vehicle> expectedResults = Arrays.asList(A, B, D);

        assertTrue(collectedResults.containsAll(expectedResults));

    }

    // from https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/testing/
    private static class CollectSink implements SinkFunction<Vehicle> {

        public static final List<Vehicle> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Vehicle value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }

        private List<Vehicle> getRecords() {
            return values;
        }
    }
}