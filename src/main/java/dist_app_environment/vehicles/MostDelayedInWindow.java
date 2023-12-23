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

/* Computes top N most delayed vehicles in a window sorted by time of update */
public class MostDelayedInWindow extends ProcessAllWindowFunction<Vehicle, Vehicle, TimeWindow> {
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
        mostDelayedVehicles.clear(); // clear state for the new window

        List<Vehicle> currentList = new ArrayList<>();

        // Add new vehicles
        for (Vehicle vehicle : elements) {
            // test if vehicle with same id is already in list
            Vehicle testIfAlreadyInList = vehicleAlreadyInList(currentList, vehicle);
            if (testIfAlreadyInList != null) {
                // remove old vehicle instance so it can be replaced by the most recent record
                currentList.remove(testIfAlreadyInList);
            }
            // add new vehicle to list
            currentList.add(vehicle);
        }

        // leave only top 5 delayed vehicles
        currentList.sort(Comparator.comparing(Vehicle::getDelay).reversed());
        currentList = currentList.subList(0, Math.min(topN, currentList.size()));

        /* sort by last update from the oldest record to the newest.
           When the timestamps are the same, it will sort by delay*/
        currentList.sort(
                Comparator.comparing(Vehicle::getLastUpdateLong).reversed()
                        .thenComparing(Vehicle::getDelay).reversed()
        );
        mostDelayedVehicles.addAll(currentList);

        // Emit the most delayed vehicles
        for (Vehicle vehicle : mostDelayedVehicles.get()) {
            out.collect(vehicle);
        }
    }

    private Vehicle vehicleAlreadyInList(List<Vehicle> elements, Vehicle v) {
        for (Vehicle alreadyIn : elements) {
            if (v.getId().equals(alreadyIn.getId())) {
                return alreadyIn;
            }
        }
        return null;
    }
}