package dist_app_environment.vehicles;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/* Keeps track of top N most delayed vehicles of all time. */
public class MostDelayedGlobally extends ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow> {
    private transient ListState<Vehicle> mostDelayedVehicles;
    private final int topN = 5;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Vehicle> descriptor =
                new ListStateDescriptor<>("mostDelayedState", TypeInformation.of(Vehicle.class));
        mostDelayedVehicles = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void process(Context context, Iterable<Vehicle> elements, Collector<Vehicle> out) throws Exception {
        // add vehicles to new list for sorting
        Iterable<Vehicle> current = mostDelayedVehicles.get();

        List<Vehicle> currentList = new ArrayList<>();
        // copy the old list to a new one
        for (Vehicle v : current) {
            currentList.add(v);
        }


        // Add new vehicles
        for (Vehicle vehicle : elements) {
            // test if vehicle with same id is already in list
            Vehicle testIfAlreadyInList = vehicleAlreadyInList(mostDelayedVehicles.get(), vehicle);
            if (testIfAlreadyInList != null) {
                // remove old vehicle instance so it can be replaced by the most recent record
                currentList.remove(testIfAlreadyInList);
            }
            // add new vehicle to list
            currentList.add(vehicle);
        }

        // sort by descending order by delay
        currentList.sort(Comparator.comparing(Vehicle::getDelay).reversed());

        mostDelayedVehicles.clear();
        // keep only topN delayed vehicles
        mostDelayedVehicles.addAll(currentList.subList(0, Math.min(topN, currentList.size())));

        // Emit the most delayed vehicles
        for (Vehicle vehicle : mostDelayedVehicles.get()) {
            out.collect(vehicle);
        }
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