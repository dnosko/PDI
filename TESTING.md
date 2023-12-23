# Testing

All tests are testing functions from class `VehicleStream`. And use mock data of class `Vehicle` with constructor expecting: \
id, vtype, bearing, lineID, lineName, delay, lastStopID, lastupdate.

## Run tests:

Tests are run by command `./gradlew test` or `./gradlew test --info` for more information. \\

## Tests

### Test: testVehiclesGoingNorth

This test is testing if function `vehiclesGoingNorth` is filtering only vehicles going north. For test is used collector to collect output data.

#### Input:

A = new Vehicle("1", (short) 1, 340, 1, "L1", 0, 10, System.currentTimeMillis()) \
B = new Vehicle("2", (short) 1, 0, 2, "L2", 0, 20, System.currentTimeMillis()) \
C = new Vehicle("3", (short) 1, 90, 1, "L1", 0, 80,System.currentTimeMillis()) \
D = new Vehicle("4", (short) 1, 45, 4, "L3", 0, 15, System.currentTimeMillis())

#### Expected result:

A,B,D, because C has bearing with value 90.

### Test: allTrainsLastStopsTest

Test for function `trainLastStop`. Test is testing if the function correctly keeps all trains since start of application and if it updates the state in case of new update for vehicle already in list. \
Test is done by using TestHarness util and models two windows by manually triggering change of processing time, therefore the timestamps are set for specific time.

#### Input:

##### First window: time 10000L

A = new Vehicle("1", (short) 5,340, 1, "L1", 0, 10, timeWindow1) \
B = new Vehicle("2", (short) 5, 0, 2, "L2", 0, 20,timeWindow1)\
C = new Vehicle("3", (short) 5, 90, 1, "L1", 0, 80,timeWindow1)\
D = new Vehicle("4", (short) 5, 45, 4, "L3", 0, 15, timeWindow1)

##### Second window: time 20000L

A2 = new Vehicle("1", (short) 5, 340, 1,"L1", 0, 20,timeWindow2)\
C2 = new Vehicle("3", (short) 5, 90, 1, "L1", 0, 80,timeWindow2)\
E = new Vehicle("5", (short) 5, 90, 5, "L5", 8, 80, timeWindow2)

#### Expected:

after first window: A,B,C,D with value 9999 \
after second window: B,D,A2,C2,E with value 19999

### Test: testDelayedVehicles

Test for function `mostDelayedVehicles`. The test is testing if the function is sorting the vehicles in the right order and when new data comes, whether the top 5 delayed vehicles are actualized accordingly. It's done through imitating two processing windows by testHarness util.

#### Input:

##### First window: time 10000

A = id:1, delay: 100, timeWindow1 \
B = id:2, delay: 60, timeWindow1\
C = id:3, delay: 10, timeWindow1\
D = id:4, delay: 50, timeWindow1 \
G = id:7, delay: 1, timeWindow1

##### Second window: time 20000

A2 = id:1, delay: 0, timeWindow2\
E = id:5, delay: 8, timeWindow2\
F = id:6, delay: 5, timeWindow2

#### Expected:

first window: A,B,D,C,G with value 9999 \
second window: B,D,C,E,F with value 19999

### Test: mostDelayedVehiclesInWindow

Testing if funciton `mostDelayedVehiclesInWindow` correctly aggregates 5 most delayed vehicles for specified window, and only for the window, therefor it shoudn't take the values from the first window into second one. Since the function takes parameter for setting the size of window function in minutes, the test isn't done for 3 minutes windows, but only for 1 minute windows. Sorting by last update and if equal then by delay is also tested. Again testHarness util is used to simulate two windows.

#### Input:

##### Window1: time 10000L

A = new Vehicle("1", (short) 5, 340, 1, "L1", 100, 10, timeWindow1)\
B = new Vehicle("2", (short) 1, 0, 2, "L2", 60, 20, timeWindow1)\
C = new Vehicle("3", (short) 5, 90, 1, "L1", 10, 80,timeWindow1)\
D = new Vehicle("4", (short) 5, 45, 4, "L3", 50, 15, timeWindow1 +1000)\
G = new Vehicle("7", (short) 5, 45, 4, "L3", 1, 15, timeWindow1+1000)\
E = new Vehicle("5", (short) 5, 90, 5, "L5", 10, 80, timeWindow1+1000)

##### Window2: time 60000L

F = new Vehicle("6", (short) 5, 90, 6, "L6", 5, 80, timeWindow2)\
I = new Vehicle("10", (short) 5, 340, 1, "L1", 90, 10, timeWindow2)\
B2 = new Vehicle("2", (short) 1, 0, 2, "L2", 70, 20, timeWindow2 +2000)\
J = new Vehicle("8", (short) 5, 90, 6, "L6", 45, 80, timeWindow2 +1000)\
K = new Vehicle("5", (short) 5, 90, 6, "L6", 15, 80, timeWindow2 +1100)\
F2 = new Vehicle("6", (short) 5, 90, 6, "L6", 5, 70, timeWindow2 + 1000);

#### Expected:

for first window: A,B,C,D,E with value 59999\
for second window: I,J,F2,K,B2 with value 119999

### Test: testGlobalAverageDelay

Testing function `averageDelay` and if average for delays is correct and is only computed within specified windows. For this reason again two windows are tested with testHarness util and it is done by triggering event time with setting watermarks and also processing time. Again, the function takes parameter for setting window size in minutes, so it's tested for 1 minute windows, not 3 minute windows.

#### Input:

Input is really by creating new instances of Vehicle class, but for better description of tests, below are show only relevant values, therefor delays.

##### Window1

delay: 100 \
delay: 60 \
delay: 10 \
delay: 50 \
delay: 1 \
delay: 8

##### Window2

delay: 5 \
delay: 100 \
delay: 70\
delay: 45 \
delay: 15 \
delay: 5

#### Expected

after first window: 38.1666666667 with value 59999
after second window: 40.0 with value 119999

### Test: averageTimeBetweenRecordsTest

Test for the function `averageTimeBetweenRecords`. Testing whether the function correctly computes the average for two different vehicles and also tests if it computes average always from last N values. Since the function takes parameter for setting the window size (by number of records), for easier testing it's not window of 10 last records, but only 5 last records.

#### Input:

Two vehicles are testing, one with id 1, which has costant difference between records, therefore the lastupdateTime is always multiplied value of 1000 by integer. Vehicle with id 2 has one more value, for testing the sliding window and also doesnt have constant time difference. \
ID 1: 1000, 2000,3000,4000,5000 \
ID 2: 1000, 1500, 3000, 3750, 5200,6200

#### Expected:

Since the values are outputed always, even when the N values weren't yet aggregated, the expected result is a sequence. When there's only one value, the output is 0.0, becuase it doesn't have anything to compare the value with. \
ID 1: 0.0, 1.0, 1.0, 1.0, 1.0, 1.0 \
ID 2: 0.0, 0.5, 1.0, 0.9166666666666666, 1.05, 1.175 \
It's however expected the values will alternate for keys - always value for id 1, then id 2, then again value for 1 and so on. Except last value, where there will be two values in a row for id 2, because id 2 has one more value than id 1. \
Since the values are aggregated in a sequence, there isn't need for specific windows, therefore the expected time value for each record is Long.MAX_VALUE, to trigger processing all elements at once.
