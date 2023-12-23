# FIT VUT 2023 PDI - Public Transport Stream Processing with Apache Flink

Data is streamed through WebSocket from ArcGIS Stream Service: [Source](https://data.brno.cz/datasets/mestobrno::polohy-vozidel-hromadn%C3%A9-dopravy-public-transit-positional-data/about) \\

## Installation

Option `--output` is optional for both local and cluster run. If it's not specified for the local run, the result is displayed in stdout. If it's not specified for run on cluster, then the output can be found at [flink local page](http://localhost:8081/#/overview) for the specified job.

### Options:

The application will run always only one of these options. \
`-h, --help` - display help \
`-1, --north` - display only vehicles going north \
`-2, --trains` - display a list of trains with their ID, their last reported station, and the time of the last update for each train reported since the start of the application \
`-3, --delayed` - display a list of 5 delayed vehicles sorted in descending order by their last reported delay since the start of the application \
`-4, --delayedw` - display a list of 5 delayed vehicles reported in the last 3 minutes, sorted in descending order by the time of their last update\
`-5, --average` - display the average delay calculated from all vehicles (both delayed and non-delayed) reported in the last 3 minutes \
`-6, -- diff`- display the average time between individual reports, considering the last 10 reports \
\
when no option or one that isn't specified here is used, the application prints the stream to stdout\

### Local run:

`./gradlew build` \
`./gradlew run` \
`java -jar build/libs/vehicles-0.1-SNAPSHOT-fat.jar [OPTIONS] [--output <out>]` \

### Run on cluster:

`./gradlew shadowJar` \
`<flink-bin-dir>/start-cluster.sh` \
`<flink-bin-dir>/flink run ./build/libs/vehicles-0.1-SNAPSHOT-all.jar [OPTIONS] [--output <out>]` \
`<flink-bin-dir>/stop-cluster.sh` \

In case of a problem, it's recommended to run `./gradlew clean` first.

## Tests

Info for testing can be found in TESTING.md.
