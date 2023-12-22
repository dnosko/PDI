package vehicles;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LastStops extends ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow> {

    private transient ListState<Vehicle> vehicles;

    @Override
    public void process(ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow>.Context context, Iterable<Vehicle> newVehicles, Collector<Vehicle> out) throws Exception {
        // access the state value
        Iterable<Vehicle> current = vehicles.get();
        //Vehicle vehicleInList = vehicleAlreadyInList(current, input);
        List<Vehicle> actualizedList = new ArrayList<>();

        // copy the old list
        for (Vehicle v : current) {
            actualizedList.add(v);
        }

        for (Vehicle vehicle : newVehicles) {
            // test if vehicle with same id is already in list
            Vehicle testIfAlreadyInList = vehicleAlreadyInList(vehicles.get(), vehicle);
            if (testIfAlreadyInList != null) {
                // update the old instance

                actualizedList.remove(testIfAlreadyInList);
            }

            actualizedList.add(vehicle);

        }

        // update the state
       vehicles.clear();
        vehicles.addAll(actualizedList);

        for (Vehicle vehicle : vehicles.get()) {
            out.collect(vehicle);
        }
    }
    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Vehicle> descriptor =
                new ListStateDescriptor<>("mostDelayedState", TypeInformation.of(Vehicle.class));
        vehicles = getRuntimeContext().getListState(descriptor);
    }

    private Vehicle vehicleAlreadyInList(Iterable<Vehicle> elements, Vehicle v) {
        for (Vehicle alreadyIn : elements) {
            if (v.getId().equals(alreadyIn.getId())) {
                return alreadyIn;
            }
        }
        return null;
    }
}