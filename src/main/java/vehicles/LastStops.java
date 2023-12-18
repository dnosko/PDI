package vehicles;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import java.time.LocalDate;
import java.util.Date;

public class LastStops extends RichFlatMapFunction<Vehicle, Vehicle>  {

    private transient ValueState<Tuple2<Long, Date>> vehicle;

    @Override
    public void flatMap(Vehicle input, Collector<Vehicle> out) throws Exception {

        // access the state value
        Tuple2<Long, Date> current = vehicle.value();

        // update the count
        current.f0 = input.laststopid;
        current.f1 = input.lastupdate;


        // update the state
        vehicle.update(current);
        out.collect(input);
        // if the count reaches 2, emit the average and clear the state
        if (current.f0 == input.finalstopid) {
            vehicle.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Date>> descriptor =
                new ValueStateDescriptor<>(
                        "laststop", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Date>>() {}), // type information
                        Tuple2.of(0L,new Date())); // default value of the state, if nothing was set
        vehicle = getRuntimeContext().getState(descriptor);
    }
}