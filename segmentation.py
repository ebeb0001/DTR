import pandas as pd
import numpy as np
import json

from shapely.geometry import LineString
from shapely.ops import transform
from pyproj import Transformer


# duplicate_tracks_id = {
#     7450119, 7458313, 7442964, 7449621, 7449622, 7449625, 7458841, 7445019, 7453211, 7437860, 7437861, 7442470, 
#     7442471, 7444006, 7444007, 7440940, 7444527, 7457330, 7457331, 7459895, 7453240, 7436860, 7436861, 7444029, 
#     7447614, 7445568, 7445569, 7449149, 7457786, 7459900, 7444037, 7442508, 7459406, 7443027, 7460443, 7460444, 
#     7437919, 7447647, 7436897, 7443553, 7453797, 7449702, 7458407, 7447662, 7447663, 7443058, 7448179, 7443578, 
#     7459456, 7447682, 7447683, 7441030, 7453320, 7450770, 7447699, 7456919, 7447705, 7458973, 7447715, 7441069, 
#     7447215, 7441584, 7438001, 7441586, 7441587, 7447216, 7442104, 7442105, 7438012, 7438013, 7441085, 7442113, 
#     7439042, 7439043, 7441092, 7441093, 7448785, 7437522, 7437523, 7459029, 7459030, 7448795, 7449311, 7447781, 
#     7453414, 7442663, 7442152, 7442664, 7448810, 7439595, 7453415, 7439597, 7453419, 7439599, 7438065, 7440639, 
#     7459583, 7447297, 7448841, 7441163, 7447819, 7441165, 7437072, 7437584, 7449360, 7437589, 7437080, 7438105, 
#     7443231, 7441184, 7441185, 7437602, 7442212, 7443750, 7459622, 7451946, 7440683, 7437616, 7446836, 7449909, 
#     7438646, 7438647, 7452474, 7450944, 7440706, 7440707, 7450445, 7456089, 7456090, 7456091, 7443300, 7443812, 
#     7452517, 7452518, 7443306, 7443308, 7446390, 7446908, 7439742, 7439743, 7456135, 7445386, 7443339, 7451020, 
#     7443341, 7451021, 7438226, 7441298, 7441814, 7441815, 7457177, 7457178, 7445403, 7447968, 7436193, 7436194, 
#     7458730, 7458733, 7440817, 7443378, 7458740, 7445438, 7449535, 7447488, 7449536, 7441349, 7441350, 7458245, 
#     7440843, 7453649, 7441881, 7441379, 7441380, 7458277, 7444970, 7451115, 7457779, 7452152, 7444986, 7447547, 
#     7442940, 7446525, 7458302, 7437823
# }

MAX_TRACK_LENGTH_M = 800.0

# projection métrique (Belgique)
to_metric = Transformer.from_crs("EPSG:4326", "EPSG:31370", always_xy=True)


def parse_coordinates(coord_str):
    try:
        coords = json.loads(coord_str)
        if isinstance(coords, list) and all(isinstance(c, list) for c in coords):
            return [[float(x) for x in c] for c in coords]
    except Exception:
        print(f"Invalid coordinate format: {coord_str}")
    return np.nan


def normalize_point(point, decimals=6):
    return (round(float(point[0]), decimals), round(float(point[1]), decimals))


def segment_length_m(p1, p2):
    """
    Distance précise via shapely + projection métrique.
    """
    line = LineString([(p1[1], p1[0]), (p2[1], p2[0])])
    line_metric = transform(to_metric.transform, line)
    return line_metric.length


def path_length_m(path):
    total = 0.0
    for i in range(len(path) - 1):
        total += segment_length_m(path[i], path[i + 1])
    return total


def split_path_on_existing_points(path, max_length_m=300.0):
    """
    Découpe uniquement sur les points existants (pas d'interpolation).
    Distances calculées avec shapely.
    """
    if not isinstance(path, list) or len(path) < 2:
        return [], []

    subpaths = []
    current_subpath = [path[0]]
    current_length = 0.0

    oversized_edges = []

    for i in range(1, len(path)):
        prev_point = path[i - 1]
        current_point = path[i]

        step_length = segment_length_m(prev_point, current_point)

        if step_length > max_length_m:
            oversized_edges.append((i - 1, i, step_length))

        if current_length + step_length <= max_length_m:
            current_subpath.append(current_point)
            current_length += step_length
        else:
            if len(current_subpath) >= 2:
                subpaths.append(current_subpath)

            current_subpath = [prev_point, current_point]
            current_length = step_length

    if len(current_subpath) >= 2:
        subpaths.append(current_subpath)

    return subpaths, oversized_edges



# Charger ton CSV
df = pd.read_csv("station_track_assigned.csv")

# Grouper par segment et compter le nombre de stations distinctes
segment_station_counts = (
    df.groupby("track_id")["Station_ID"]
    .nunique()
    .reset_index()
)

# Garder uniquement les segments liés à plusieurs stations
segments_multi_stations = segment_station_counts[
    segment_station_counts["Station_ID"] > 1
]

# Récupérer les IDs
result = segments_multi_stations["track_id"].tolist()

print(f"Nombre total de segments : {len(df['track_id'].unique())}")
print(f"Segments liés à plusieurs stations : {len(set(result))}")

result_set = set([int(x) for x in result])
# ------------------- PIPELINE -------------------

main_tracks_df = pd.read_csv("cleaned_data/main_tracks.csv", sep=";")
main_tracks_df["Coordinates"] = main_tracks_df["Coordinates"].apply(parse_coordinates)

# switches_df = pd.DataFrame(columns=["ID", "X", "Y", "Elevation"])
switches_df = pd.DataFrame(columns=["ID", "X", "Y"])
switches = {}
tracks_rows = []

node_id = 0
track_id = 0

for _, row in main_tracks_df.iterrows():
    path = row["Coordinates"]

    if not isinstance(path, list) or len(path) < 2:
        continue

    if int(row["ID"]) in result_set:
        split_paths, _ = split_path_on_existing_points(path, MAX_TRACK_LENGTH_M)

        for subpath in split_paths:
            start = normalize_point(subpath[0])
            end = normalize_point(subpath[-1])

            if start not in switches:
                switches[start] = node_id
                # switches_df.loc[len(switches_df)] = [node_id, start[0], start[1], start[2]]
                switches_df.loc[len(switches_df)] = [node_id, start[0], start[1]]
                node_id += 1

            if end not in switches:
                switches[end] = node_id
                # switches_df.loc[len(switches_df)] = [node_id, end[0], end[1], end[2]]
                switches_df.loc[len(switches_df)] = [node_id, end[0], end[1]]
                node_id += 1

            tracks_rows.append({
                "ID": track_id,
                "Departure_switch": switches[start],
                "Arrival_switch": switches[end],
                "Path": json.dumps(subpath),
                "Length_m": path_length_m(subpath)
            })

            track_id += 1
    else :
        start = normalize_point(path[0])
        end = normalize_point(path[-1])

        if start not in switches:
            switches[start] = node_id
            # switches_df.loc[len(switches_df)] = [node_id, start[0], start[1], start[2]]
            switches_df.loc[len(switches_df)] = [node_id, start[0], start[1]]
            node_id += 1

        if end not in switches:
            switches[end] = node_id
            # switches_df.loc[len(switches_df)] = [node_id, end[0], end[1], end[2]]
            switches_df.loc[len(switches_df)] = [node_id, end[0], end[1]]
            node_id += 1

        tracks_rows.append({
            "ID": track_id,
            "Departure_switch": switches[start],
            "Arrival_switch": switches[end],
            "Path": json.dumps(path),
            "Length_m": path_length_m(path)
        })

        track_id += 1


tracks_df = pd.DataFrame(tracks_rows)

switches_df.to_csv("sumo_data/switches.csv", index=False, sep=";")
tracks_df.to_csv("sumo_data/main_tracks.csv", index=False, sep=";")
# print(set(result))