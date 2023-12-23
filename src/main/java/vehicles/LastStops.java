package vehicles;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

/* Keeps track of all recorded vehicles and updates the ones already in the list with updated values.  */
public class LastStops extends ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow> {

    private transient ListState<Vehicle> vehicles;

    @Override
    public void process(ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow>.Context context, Iterable<Vehicle> newVehicles, Collector<Vehicle> out) throws Exception {
        Iterable<Vehicle> current = vehicles.get();

        List<Vehicle> actualizedList = new ArrayList<>();

        // copy the old list to the new one
        for (Vehicle v : current) {
            actualizedList.add(v);
        }

        // add new vehicles to list
        for (Vehicle vehicle : newVehicles) {
            // test if vehicle with same id is already in list
            Vehicle testIfAlreadyInList = vehicleAlreadyInList(vehicles.get(), vehicle);
            if (testIfAlreadyInList != null) {
                // remove the old instance
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

    /* Checks if vehicle v is already in list, if it is then it returns the instance that is in the list. */
    private Vehicle vehicleAlreadyInList(Iterable<Vehicle> elements, Vehicle v) {
        for (Vehicle alreadyIn : elements) {
            if (v.getId().equals(alreadyIn.getId())) {
                return alreadyIn;
            }
        }
        return null;
    }
}