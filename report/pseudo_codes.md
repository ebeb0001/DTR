#### Algorithm 1: Macro-level railway network generation

```text
Input:
    S: set of railway stations
    C: set of station-to-station railway connections
    selected_stations: stations selected by the user

Output:
    SUMO network files

1. Initialize an empty set of nodes N
2. Initialize an empty set of edges E

3. For each station s in S:
       If s belongs to selected_stations or is required for connectivity:
           Create a SUMO node using the station identifier and coordinates
           Add the node to N

4. For each connection c in C:
       Let origin = c.departure_station
       Let destination = c.arrival_station

       If origin and destination are both included in N:
           Create a SUMO edge between origin and destination
           Assign the distance and railway geometry to the edge
           Add the edge to E

5. Export N as a SUMO node file
6. Export E as a SUMO edge file
7. Generate additional SUMO files for stops, routes, and configuration
8. Run netconvert to create the final SUMO network

Return generated SUMO network files
```

#### Algorithm 2: Macro-level trip reconstruction from real operational data

```text
Input:
    P: punctuality dataset
    simulation_start_date
    simulation_duration

Output:
    Set of SUMO train routes

1. Filter P according to the selected simulation period
2. Remove incomplete or inconsistent records

3. For each train identifier and operating day:
       Extract all station records associated with the train
       Sort the records by planned departure time

       Initialize an empty route R

       For each consecutive pair of stations (si, sj):
           If si and sj are connected in the macro-level network:
               Add sj to route R

       Convert the first planned departure time into SUMO simulation time
       Create a SUMO train route using R and the computed departure time

4. Export all reconstructed routes to a SUMO route file

Return SUMO route file
```

#### Algorithm 3: Switch detection in the micro-level network

```text
Input:
    T: set of railway track segments

Output:
    SW: set of detected switches

1. Initialize an empty adjacency dictionary A
2. Initialize an empty set of switches SW

3. For each track segment t in T:
       Extract the start node and end node of t
       Add t to the adjacency list of its start node
       Add t to the adjacency list of its end node

4. For each node n in A:
       Compute degree(n) as the number of connected track segments

       If degree(n) >= 3:
           Classify n as a switch
           Add n to SW

5. Return SW
```

#### Algorithm 4: Platform detection by spatial proximity

```text
Input:
    S: set of railway stations
    T: set of railway track segments
    platform_count(s): number of platforms for each station

Output:
    Platform-to-track associations

1. Initialize an empty association table A

2. For each station s in S:
       Define a search area around the station coordinates
       Select all track segments located inside or near this area

       For each candidate track segment t:
           Compute the distance between s and t

       Sort candidate segments by increasing distance

       Let n = platform_count(s)
       Select the n closest track segments

       For each selected segment:
           Associate the segment with one platform of station s
           Store the association in A

3. Return A
```

#### Algorithm 5: Track segmentation for conflicting platform assignments

```text
Input:
    T: set of railway track segments
    A: platform-to-track associations
    max_length: maximum allowed segment length

Output:
    Updated set of railway track segments

1. Identify all track segments assigned to more than one station
2. For each conflicting segment t:
       Extract the geometry of t as an ordered sequence of coordinates
       Compute the length of t

       If length(t) > max_length:
           Split t into smaller sub-segments following the original geometry
           Create new nodes at the boundaries of the sub-segments
           Replace t with the generated sub-segments

3. Update the railway track dataset
4. Repeat platform detection on the updated segments

Return updated railway track segments
```

#### Algorithm 6: Dual graph construction for micro-level routing

```text
Input:
    T: set of railway track segments

Output:
    Dual graph G

1. Initialize an empty directed graph G
2. Add each track segment t in T as a node in G

3. Create a lookup table L:
       For each infrastructure node n:
           Store all track segments connected to n

4. For each track segment ti in T:
       Let end(ti) be the destination node of ti

       For each track segment tj connected to end(ti):
           If tj can be reached from ti:
               Add a directed edge from ti to tj in G
               Assign the length of ti as edge weight

5. Return G
```

#### Algorithm 7: Micro-level route computation between two stations

```text
Input:
    origin_station
    destination_station
    platform_associations
    dual_graph G

Output:
    Best railway route between the two stations

1. Retrieve all platform tracks associated with origin_station
2. Retrieve all platform tracks associated with destination_station

3. Initialize best_route as empty
4. Initialize best_distance as infinity

5. For each origin platform track po:
       For each destination platform track pd:
           Compute the shortest path from po to pd in G using Dijkstra's algorithm

           If a valid path exists and its distance is lower than best_distance:
               Store this path as best_route
               Update best_distance

6. Convert the selected sequence of track segments into SUMO edge identifiers

Return best_route
```

#### Algorithm 8: Random-trip generation and validation

```text
Input:
    SUMO network
    candidate_platforms
    reconstructed_train_paths
    simulation_duration

Output:
    Validated SUMO route file

1. Generate edge weighting files using candidate station platforms
2. Exclude non-platform railway edges from possible origins and destinations

3. Run randomTrips.py with railway-specific parameters:
       - simulation duration
       - insertion rate
       - minimum trip distance
       - candidate edge weights

4. For each generated trip:
       Map the origin edge to its corresponding station
       Map the destination edge to its corresponding station

       If the station pair exists in reconstructed train paths:
           Keep the trip
       Else:
           Discard the trip

5. Run duarouter to compute complete routes for the retained trips
6. Export the validated route file

Return validated SUMO route file
```
