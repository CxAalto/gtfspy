Query list:

From a transit schedule database we want to have the following efficient (nontrivial) queries:

Why unix time?
It can be easily mapped to/and from from any date.

Suitable for incremental incraeses in time by simple
"time += more_time" -like operations.
Easy sorting of things by thing.

1. Get all scheduled trips (e.g. for visualization purposes)
  i) starting
  i) ending
  i) going on
  during an time interval (expressed in unix time in seconds)

  A trip consists of:
    - latitudes
    - longitudes
    - route_type (= vehicle type)
    - times

2. Get all trips from one stop (or groups of stops) during an unix time interval.

3. Get number of trips between two stops within a unix time interval.

4. Get number of stops at a transit access point within a unix time interval.

5. Get transfer probabilities from a bus line.

6. Get transfer probabilities from a specific trip.

7. Get static network:
  Create a static network, with links of different types.
  Link types:
    Static routing
  Link types:


Tables sketches:

trips:
trip_id (primary key, starting point)
unix_start_time (in unix)
unix_end_time (in unix)
route_id
shape_id
trip_name

stop_times:
trip_id
stop_time (unix time after epoch)
stop_id
shape_break_seq
seq

stops:
stop_id
lat
lng
name

shapes:
shape_id seq lat lon
