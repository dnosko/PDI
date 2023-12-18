package vehicles;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AllTimeDelayed extends RichFlatMapFunction<Vehicle, Vehicle> {

    private transient ListState<Vehicle> delayedVehicles;
    private int topN = 5;

    @Override
    public void flatMap(Vehicle input, Collector<Vehicle> out) throws Exception {

        Iterable<Vehicle> current = delayedVehicles.get();
        List<Vehicle> currentList = new ArrayList<>();
        boolean isTheSameVehicle = false;
        for (Vehicle v : current) {
            if (v.id == input.id){
                currentList.add(input); // if the same vehicle, add the most actual one
                isTheSameVehicle = true;
            }
            else
                currentList.add(v);
        }
        // add new vehicle to list
        if (!isTheSameVehicle)
            currentList.add(input);


        // sort the list by delay
        currentList.sort(Comparator.comparingDouble(Vehicle::getDelay).reversed());
        // keep size of the list to only top N delayed values
        if (currentList.size() > 5) {
                currentList = new ArrayList<>(currentList.subList(0, topN));
        }


        // update the state
        delayedVehicles.update(currentList);
        for (Vehicle v: currentList)
            out.collect(v);
    }

    @Override
    public void open(Configuration config) {
        ListStateDescriptor<Vehicle> descriptor =
                new ListStateDescriptor<>(
                        "laststop", // the state name
                        TypeInformation.of(Vehicle.class)); // type information for the elements in the list
        delayedVehicles = getRuntimeContext().getListState(descriptor);
    }
}