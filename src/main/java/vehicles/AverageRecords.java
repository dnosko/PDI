package vehicles;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageRecords
        implements AggregateFunction<Vehicle, Tuple4<String, Long, Long, Long>, Tuple2<String,Double>> {

    @Override
    public Tuple4<String, Long, Long, Long> createAccumulator() {
        // key, count, sum, timestamp
        return Tuple4.of("",0L, 0L, 0L);
    }

    @Override
    public Tuple4<String, Long, Long, Long> add(Vehicle vehicle, Tuple4<String, Long, Long, Long> accumulator) {
        long currentTimestamp = vehicle.getLastUpdateLong();

        if (accumulator.f3 > 0) {
            // Calculate time difference between current and previous timestamp
            long timeDifference = currentTimestamp - accumulator.f3;

            // Update accumulator
            return Tuple4.of(vehicle.id, accumulator.f1 + 1L, accumulator.f2 + timeDifference, currentTimestamp);
        } else {
            // first record
            return Tuple4.of(vehicle.id, 0L, 0L, currentTimestamp);
        }
    }

    @Override
    public Tuple2<String,Double> getResult(Tuple4<String, Long, Long, Long> accumulator) {
        // Calculate the average time
        double averageTime;

        if (accumulator.f1 == 0) {
            averageTime = 0.0; // first record, there is no difference yet.
        }
        else {
           averageTime = (double) accumulator.f2 / (double) accumulator.f1;
        }
        double inSeconds = averageTime/1000;
        return new Tuple2<>(accumulator.f0, inSeconds);
    }

    @Override
    public Tuple4<String, Long, Long, Long> merge(Tuple4<String, Long, Long, Long> a, Tuple4<String, Long, Long, Long> b) {
        System.out.println("Merging:" + a.f0 + " " + a.f3 + " " + b.f0 + " " + b.f3);
        return Tuple4.of(a.f0, a.f1 + b.f1, a.f2 + b.f2, Math.max(a.f3, b.f3));
    }
}
