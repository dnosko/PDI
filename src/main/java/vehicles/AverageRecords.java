/**
 * Body from: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/windows/
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
