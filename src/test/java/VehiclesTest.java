package vehicles;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.operators.*;

import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.*;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import java.util.concurrent.ConcurrentLinkedQueue;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;



public class VehiclesTest {
    Main main = new Main();

    // body taken from https://github.com/apache/flink/blob/master/flink-streaming-java/src/test/java/org/apache/flink/streaming/runtime/operators/windowing/AllWindowTranslationTest.java#L1307
    private static <K, OUT> ConcurrentLinkedQueue<Object> processElementAndEnsureOutput(
            OneInputStreamOperator<Vehicle, OUT> operator,
            KeySelector<Vehicle, K> keySelector,
            TypeInformation<K> keyType,
            List<Vehicle> elementsWindow1, List<Vehicle> elementsWindow2, Long firstWindowEndTime)
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<K, Vehicle, OUT> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);

        testHarness.open();

        testHarness.setProcessingTime(0);
        //testHarness.processWatermark(Integer.MIN_VALUE);

        for(Vehicle element: elementsWindow1)
            testHarness.processElement(new StreamRecord<>(element));

        // provoke any processing-time/event-time triggers
        testHarness.setProcessingTime(firstWindowEndTime);
        //testHarness.processWatermark(firstWindowEndTime);

        ConcurrentLinkedQueue<Object> expected = testHarness.getOutput();

        for (Object el : expected) {
            System.out.println("First window:" + el);
        }
        assertEquals(testHarness.getOutput().size(), 5);

        for(Vehicle element: elementsWindow2)
            testHarness.processElement(new StreamRecord<>(element));


        long secondWindowEndTime = Integer.MAX_VALUE;
        testHarness.setProcessingTime(secondWindowEndTime);
        //testHarness.processWatermark(secondWindowEndTime);
        expected = testHarness.getOutput();


        for (Object el : expected) {
            System.out.println("Secondd window:" + el);
        }

        // check if it outputed together 10 resulsts
        assertEquals(testHarness.getOutput().size(), 10);

        testHarness.close();
        return expected;
    }

    private static <K, IN, OUT> ConcurrentLinkedQueue<Object> processElementAndEnsureOutputAverage(
            OneInputStreamOperator<IN, OUT> operator,
            KeySelector<IN, K> keySelector,
            TypeInformation<K> keyType,
            List<IN> elementsWindow1, List<IN> elementsWindow2, Long firstWindowEndTime)
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);

        testHarness.open();

        testHarness.setProcessingTime(0);
        testHarness.processWatermark(0);

        for(IN element: elementsWindow1) {
            System.out.println(element);
            testHarness.processElement(new StreamRecord<>(element,10000L));
        }

        // provoke any processing-time/event-time triggers
        testHarness.setProcessingTime(10050L);
        testHarness.processWatermark(10050L);

        ConcurrentLinkedQueue<Object> expected = testHarness.getOutput();

        System.out.println("output:" + testHarness.getOutput());

        for (Object el : expected) {
            System.out.println("First window:" + el);
        }
        //assertEquals(testHarness.getOutput().size(), 1);

        for(IN element: elementsWindow2) {
            System.out.println(element);
            testHarness.processElement(new StreamRecord<>(element, 70000L));
        }


        long secondWindowEndTime = Integer.MAX_VALUE;
        testHarness.setProcessingTime(secondWindowEndTime);
        testHarness.processWatermark(Integer.MAX_VALUE);
        expected = testHarness.getOutput();

        ConcurrentLinkedQueue<Object> expectedResultsOnly = new ConcurrentLinkedQueue<>();

        for (Object el : expected) {
            System.out.println("Secondd window:" + el);
            if (el instanceof StreamRecord) {
                expectedResultsOnly.add(el);
            }
        }
        // check if it records are two..
        assertEquals(2,  expectedResultsOnly.size());

        testHarness.close();
        return  expectedResultsOnly;
    }

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

    @Test
    public void testDelayedVehicles()  throws Exception {
        long timeWindow1 = 10000L;
        long timeWindow2 = 20000L;
        long firstWindowEndTime = 10050L;
        Vehicle A = new Vehicle("1", (short) 5, 340, 1, "L1", 100, 10, timeWindow1);
        Vehicle B = new Vehicle("2", (short) 1, 0, 2, "L2", 60, 20, timeWindow1);
        Vehicle C = new Vehicle("3", (short) 5, 90, 1, "L1", 10, 80,timeWindow1);
        Vehicle D = new Vehicle("4", (short) 5, 45, 4, "L3", 50, 15, timeWindow1 );
        Vehicle G = new Vehicle("7", (short) 5, 45, 4, "L3", 1, 15, timeWindow1);
        Vehicle A2 = new Vehicle("1", (short) 5, 340, 1, "L1", 0, 20, timeWindow2);
        Vehicle E = new Vehicle("5", (short) 5, 90, 5, "L5", 8, 80, timeWindow2);
        Vehicle F = new Vehicle("6", (short) 5, 90, 6, "L6", 5, 80, timeWindow2);

        List<Vehicle> input = Arrays.asList(A, B, C, D, G);
        List<Vehicle> input2 = List.of(A2,E,F);

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        // first window
        expected.add(new StreamRecord<>(A,9999));
        expected.add(new StreamRecord<>(B,9999));
        expected.add(new StreamRecord<>(D,9999));
        expected.add(new StreamRecord<>(C,9999));
        expected.add(new StreamRecord<>(G,9999));
        // second window
        expected.add(new StreamRecord<>(B,19999));
        expected.add(new StreamRecord<>(D,19999));
        expected.add(new StreamRecord<>(C,19999));
        expected.add(new StreamRecord<>(E,19999));
        expected.add(new StreamRecord<>(F,19999));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend());


        DataStream<Vehicle> inputStream = env.fromCollection(input);
        DataStream<Vehicle> window1 = Main.mostDelayedVehicles(inputStream);

        OneInputTransformation<Vehicle, Vehicle> transform =
                (OneInputTransformation<Vehicle, Vehicle>)
                        window1.getTransformation();
        OneInputStreamOperator<Vehicle, Vehicle> operator =
                transform.getOperator();
        assertTrue(operator instanceof WindowOperator);
        WindowOperator<Vehicle, Vehicle, ?, ?, ?> winOperator =
                (WindowOperator<Vehicle, Vehicle, ?, ?, ?>) operator;

        ConcurrentLinkedQueue<Object> results = processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                TypeInformation.of(Vehicle.class),
                input, input2, firstWindowEndTime);


        TestHarnessUtil.assertOutputEqualsSorted("Output not equal to expected", expected, results,
                Comparator.comparing(streamRecord -> ((StreamRecord<Vehicle>) streamRecord).getValue().getId())
        );
    }

    @Test
    public void mostDelayedVehiclesInWindow()  throws Exception {
        long timeWindow1 = 10000L;
        long timeWindow2 = 60000L;
        long firstWindowEndTime = 60050L;
        // result A,B,D,C,E
        Vehicle A = new Vehicle("1", (short) 5, 340, 1, "L1", 100, 10, timeWindow1);
        Vehicle B = new Vehicle("2", (short) 1, 0, 2, "L2", 60, 20, timeWindow1);
        Vehicle C = new Vehicle("3", (short) 5, 90, 1, "L1", 10, 80,timeWindow1);
        Vehicle D = new Vehicle("4", (short) 5, 45, 4, "L3", 50, 15, timeWindow1 +1000);
        Vehicle G = new Vehicle("7", (short) 5, 45, 4, "L3", 1, 15, timeWindow1+1000);
        Vehicle E = new Vehicle("5", (short) 5, 90, 5, "L5", 8, 80, timeWindow1+1000);

        // result A2,F2,B2,J,K
        Vehicle F = new Vehicle("6", (short) 5, 90, 6, "L6", 5, 80, timeWindow2);
        Vehicle A2 = new Vehicle("1", (short) 5, 340, 1, "L1", 100, 10, timeWindow2);
        Vehicle B2 = new Vehicle("2", (short) 1, 0, 2, "L2", 70, 20, timeWindow2);
        Vehicle J = new Vehicle("8", (short) 5, 90, 6, "L6", 45, 80, timeWindow2 +1000);
        Vehicle K = new Vehicle("5", (short) 5, 90, 6, "L6", 15, 80, timeWindow2 +1000);
        Vehicle F2 = new Vehicle("6", (short) 5, 90, 6, "L6", 5, 70, timeWindow2 + 1000);

        List<Vehicle> input = Arrays.asList(A, B, C, D, G, E);
        List<Vehicle> input2 = Arrays.asList(F,A2,B2,J,K,F2);

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        // first window
        expected.add(new StreamRecord<>(A,59999));
        expected.add(new StreamRecord<>(B,59999));
        expected.add(new StreamRecord<>(D,59999));
        expected.add(new StreamRecord<>(C,59999));
        expected.add(new StreamRecord<>(E,59999));
        // second window 59999
        expected.add(new StreamRecord<>(A2,119999));
        expected.add(new StreamRecord<>(F2,119999));
        expected.add(new StreamRecord<>(B2,119999));
        expected.add(new StreamRecord<>(J,119999));
        expected.add(new StreamRecord<>(K,119999));


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism as needed
        env.setStateBackend(new MemoryStateBackend());


        DataStream<Vehicle> inputStream = env.fromCollection(input);
        DataStream<Vehicle> window1 = Main.mostDelayedVehiclesInWindow(inputStream, 1);

        OneInputTransformation<Vehicle, Vehicle> transform =
                (OneInputTransformation<Vehicle, Vehicle>)
                        window1.getTransformation();
        OneInputStreamOperator<Vehicle, Vehicle> operator =
                transform.getOperator();
        assertTrue(operator instanceof WindowOperator);
        WindowOperator<Vehicle, Vehicle, ?, ?, ?> winOperator =
                (WindowOperator<Vehicle, Vehicle, ?, ?, ?>) operator;

        ConcurrentLinkedQueue<Object> results = processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                TypeInformation.of(Vehicle.class),
                input, input2 ,firstWindowEndTime);

        TestHarnessUtil.assertOutputEqualsSorted("Output not equal to expected", expected, results,
                Comparator.comparing(streamRecord -> ((StreamRecord<Vehicle>) streamRecord).getValue().getId())
        );
    }

    @Test
    public void testGlobalAverageDelay() throws Exception {
        long timeWindow1 = 10000L;
        long timeWindow2 = 60000L;
        long firstWindowEndTime = 60050L;
        int minute = 1;
        // result 100 + 60 + 10 + 50 + 1 + 8 / 6 = 38.1666666667
        Vehicle A = new Vehicle("1", (short) 5, 340, 1, "L1", 100, 10, timeWindow1);
        Vehicle B = new Vehicle("2", (short) 1, 0, 2, "L2", 60, 20, timeWindow1);
        Vehicle C = new Vehicle("3", (short) 5, 90, 1, "L1", 10, 80,timeWindow1);
        Vehicle D = new Vehicle("4", (short) 5, 45, 4, "L3", 50, 15, timeWindow1 +1000);
        Vehicle G = new Vehicle("7", (short) 5, 45, 4, "L3", 1, 15, timeWindow1+1000);
        Vehicle E = new Vehicle("5", (short) 5, 90, 5, "L5", 8, 80, timeWindow1+1000);

        // result A2,F2,B2,J,K
        Vehicle F = new Vehicle("6", (short) 5, 90, 6, "L6", 5, 80, timeWindow2);
        Vehicle A2 = new Vehicle("1", (short) 5, 340, 1, "L1", 100, 10, timeWindow2);
        Vehicle B2 = new Vehicle("2", (short) 1, 0, 2, "L2", 70, 20, timeWindow2);
        Vehicle J = new Vehicle("8", (short) 5, 90, 6, "L6", 45, 80, timeWindow2 +1000);
        Vehicle K = new Vehicle("5", (short) 5, 90, 6, "L6", 15, 80, timeWindow2 +1000);
        Vehicle F2 = new Vehicle("6", (short) 5, 90, 6, "L6", 5, 70, timeWindow2 + 1000);

        List<Vehicle> input = Arrays.asList(A, B, C, D, G, E);
        List<Vehicle> input2 = Arrays.asList(F,A2,B2,J,K,F2);


        double averageWindow1 = (A.delay + B.delay + C.delay + D.delay + G.delay + E.delay) / input.size();
        double averageWindow2 = (F.delay + A2.delay + B2.delay + J.delay + K.delay + F2.delay) / input2.size();

        // expected results
        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord<>(averageWindow1,59999));
        expected.add(new StreamRecord<>(averageWindow2,119999));


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism as needed
        env.setStateBackend(new MemoryStateBackend());


        DataStream<Vehicle> inputStream = env.fromCollection(input);
        DataStream<Double> window1 = Main.averageDelay(inputStream, minute);

        OneInputTransformation<Vehicle, Double> transform =
                (OneInputTransformation<Vehicle, Double>) window1.getTransformation();
        OneInputStreamOperator<Vehicle, Double> operator =
                transform.getOperator();
        assertTrue(operator instanceof WindowOperator);
        WindowOperator<Vehicle, Vehicle, ?, ?, ?> winOperator =
                (WindowOperator<Vehicle, Vehicle, ?, ?, ?>) operator;

        ConcurrentLinkedQueue<Object> results = processElementAndEnsureOutputAverage(
                winOperator,
                winOperator.getKeySelector(),
                TypeInformation.of(Vehicle.class),
                input, input2 ,firstWindowEndTime);

        TestHarnessUtil.assertOutputEqualsSorted("Output not equal to expected", expected, results,
                Comparator.comparing(streamRecord -> ((StreamRecord<Double>) streamRecord).getValue())
        );
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