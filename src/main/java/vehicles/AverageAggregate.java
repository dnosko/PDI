package vehicles;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Body from: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/windows/
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 */
public class AverageAggregate implements AggregateFunction<Vehicle, Tuple2<Double, Long>, Double> {
    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return new Tuple2<>(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(Vehicle vehicle, Tuple2<Double, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 + vehicle.delay, accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> accumulator) {
        return ((double) accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
