import pandas as pd
import numpy as np
import json
import argparse
from collections import defaultdict
from copy import deepcopy
import math
from pyproj import Transformer

class LineSegmentSeparator :
	def __init__(self, filename : str, separator : str, output_dir : str) :
		self.filename : str = filename
		self.separator : str = separator
		self.output_dir : str = output_dir
		self.output_filenames : dict[str : str] = {
			"tracks" : f"{self.output_dir}/main_tracks.csv",
			"switches" : f"{self.output_dir}/switches.csv",
		}
		self.dataframe : pd.DataFrame = None 
		self.switches : dict = dict()
		self.graph : defaultdict = defaultdict(set)
		self.simplified_graph : defaultdict = defaultdict(set)
		self.tracks : defaultdict = defaultdict(dict) 
		self.transformer : Transformer = Transformer.from_crs("EPSG:4326", "EPSG:32631", always_xy=True)

	def parseCoordinates(self, coord_str : str) -> list[list[float]] | None :
		try:
			coords = json.loads(coord_str)
			if isinstance(coords, list) and all(isinstance(c, list) for c in coords):
				return [[float(x) for x in c] for c in coords]
		except Exception:
			print(f"Invalid coordinate format: {coord_str}")
		return np.nan

	def extractData(self) -> None :
		print("Extracting data...")
		self.dataframe = pd.read_csv(self.filename, sep=self.separator)
		self.dataframe["Coordinates"] = self.dataframe["Coordinates"].apply(self.parseCoordinates)
		print("Data extraction completed successfully.")

	def normalize_point(self, p : tuple[float, float, float]) -> tuple[float, float, float]:
		lon = round(p[0], 6)
		lat = round(p[1], 6)
		z = round(p[2], 2) if len(p) > 2 else 0.0
		return (lon, lat, z)

	def buildGraph(self) -> None :
		print("Building graph...")
		for _, row in self.dataframe.iterrows():
			coords = row["Coordinates"]
			if isinstance(coords, list) and len(coords) > 1:
				norm_coords = [self.normalize_point(tuple(c)) for c in coords]
				for i in range(len(norm_coords) - 1):
					p1 = norm_coords[i]
					p2 = norm_coords[i + 1]
					if p1 != p2:
						self.graph[p1].add(p2)
						self.graph[p2].add(p1)
		print("Graph building completed successfully.")

	def edgeKey(self, p1 : tuple[float, float, float], 
	p2 : tuple[float, float, float]) -> tuple[tuple[float, float, float], tuple[float, float, float]] :
		return frozenset((p1, p2))
		# return (p1, p2) if p1 < p2 else (p2, p1)

	def angle_between(self, previous, current, next_) -> float :
		previous = self.transformer.transform(previous[1], previous[0])
		current = self.transformer.transform(current[1], current[0])
		next_ = self.transformer.transform(next_[1], next_[0])
		x1, y1 = current[0] - previous[0], current[1] - previous[1]
		x2, y2 = next_[0] - current[0], next_[1] - current[1]

		n1 = math.hypot(x1, y1)
		n2 = math.hypot(x2, y2)
		if n1 == 0 or n2 == 0:
			return None

		cos_theta = (x1 * x2 + y1 * y2) / (n1 * n2)
		cos_theta = max(-1.0, min(1.0, cos_theta))
		return math.degrees(math.acos(cos_theta))

	def contract_2_degree_nodes(self) -> dict[str, list[dict[str :  list[tuple[float, float, float]]
	| tuple[float, float, float]]]] :
		print("Contracting degree-2 nodes...")
		degrees = {node: len(neighbors) for node, neighbors in self.graph.items()}

		# Nœuds topologiques = tous ceux qui ne sont PAS de degré 2
		topological_nodes = {node for node, deg in degrees.items() if deg != 2}

		contracted_edges = []
		visited_half_edges = set()

		# Parcours à partir de chaque nœud topologique
		for start in topological_nodes:
			for neighbor in self.graph[start]:
				hk = self.edgeKey(start, neighbor)
				if hk in visited_half_edges:
					continue

				path = [start]
				prev = start
				current = neighbor

				visited_half_edges.add(hk)

				while True:
					path.append(current)

					# Si on atteint un autre nœud topologique, on termine
					if current in topological_nodes:
						break

					# Sinon current est censé être de degré 2
					neighbors = list(self.graph[current])
					if len(neighbors) != 2:
						# Cas anormal, on arrête proprement
						break

					candidates = [n for n in neighbors if n != prev]
					if not candidates:
						break

					next_node = candidates[0]

					angle = self.angle_between(prev, current, next_node)
					if angle is None:
						break

					# à ajuster selon tes données
					if not (0.0 <= angle <= 45.0):
						break

					hk = self.edgeKey(current, next_node)
					if hk in visited_half_edges:
						# Évite de tourner en boucle
						break

					visited_half_edges.add(hk)
					prev, current = current, next_node

				contracted_edges.append({
					"start": path[0],
					"end": path[-1],
					"path": path,
					"internal_degree_2_nodes": path[1:-1]
				})

		# Cas particulier : cycles composés uniquement de nœuds de degré 2
		# Exemple : A-B-C-D-A avec deg=2 partout
		special_cycles = []
		visited_cycle_nodes = set()
		degree_2_nodes = {node for node, deg in degrees.items() if deg == 2}
		for node in degree_2_nodes:
			if node in visited_cycle_nodes:
				continue
			# On explore la composante formée de nœuds de degré 2
			component = []
			stack = [node]
			local_seen = set()
			while stack:
				# print("stack")
				u = stack.pop()
				# print(u, u in local_seen)
				if u in local_seen:
					continue
				local_seen.add(u)
				component.append(u)
				for v in self.graph[u]:
					if v in degree_2_nodes:
						stack.append(v)
			# print("out", len(visited_cycle_nodes))
			# Vérifie si toute la composante est un cycle pur :
			# chaque nœud a exactement 2 voisins ET tous les voisins sont dans la composante
			comp_set = set(component)
			is_pure_cycle = True
			for u in component:
				if len(self.graph[u]) != 2:
					is_pure_cycle = False
					break
				if not all(v in comp_set for v in self.graph[u]):
					is_pure_cycle = False
					break
			if is_pure_cycle:
				# Reconstruit un ordre de cycle
				ordered_cycle = []
				start = component[0]
				neighbors = list(self.graph[start])
				prev = None
				current = start
				while True:
					ordered_cycle.append(current)
					visited_cycle_nodes.add(current)
					next_candidates = [v for v in self.graph[current] if v != prev]
					if not next_candidates:
						break
					next_node = next_candidates[0]
					prev, current = current, next_node
					if current == start:
						break
					if current in ordered_cycle:
						break
				special_cycles.append({
					"cycle_nodes": ordered_cycle
				})
			else:
				visited_cycle_nodes.update(component)
		return contracted_edges, special_cycles

	def buildSimplifiedGraph(self) -> None :
		print("Building simplified graph...")
		contracted_edges, cycles = self.contract_2_degree_nodes()
		for segment in contracted_edges:
			a = segment["start"]
			b = segment["end"]

			if a != b:
				self.simplified_graph[a].add(b)
				self.simplified_graph[b].add(a)
				link = self.edgeKey(a, b)
				if link not in self.tracks :
					self.tracks[link] = []
				self.tracks[link].append(segment["path"])
		for cycle in cycles:
			for i in range(len(cycle) - 1):
				a = cycle[i]
				b = cycle[i + 1]
				if a != b:
					self.simplified_graph[a].add(b)
					self.simplified_graph[b].add(a)
					link = self.edgeKey(a, b)
					if link not in self.tracks :
						self.tracks[link] = None
		print("Simplified graph building completed successfully.")

	def loadSwitches(self) -> None :	
		print("Loading switches dataframe...")
		node_id : int = 0
		switche_df : pd.DataFrame = pd.DataFrame(columns=["ID", "X", "Y", "Elevation"])
		for node in self.simplified_graph :
				switche_df.loc[node_id] = [node_id, node[0], node[1], node[2]]
				self.switches[node] = node_id
				node_id += 1
		switche_df = switche_df.astype({
			"ID" : int,
			"X" : float,
			"Y" : float,
			"Elevation" : float
		})
		switche_df.to_csv(self.output_filenames["switches"], index=False)

	def loadTracks(self) -> None :
		print("Loading main tracks dataframe...")
		track_id : int = 0
		tracks_df : pd.DataFrame = pd.DataFrame(columns=["ID", "Departure_switch", "Arrival_switch", "Path"])
		for link in self.tracks :
			a, b = link
			depart_switch_id = self.switches[a]
			arrival_switch_id = self.switches[b]
			for p in self.tracks[link] :
				path = [list(node) for node in p] if p else None
				tracks_df.loc[track_id] = [track_id, depart_switch_id, arrival_switch_id, path]
				track_id += 1
		tracks_df = tracks_df.astype({
			"ID" : int,
			"Departure_switch" : int,
			"Arrival_switch" : int,
			"Path" : object
		})
		tracks_df.to_csv(self.output_filenames["tracks"], index=False, sep=";")

	def loadData(self) -> None :
		print("Loading data into csv files...")
		self.loadSwitches()
		self.loadTracks()
		print("Data loading completed successfully.")

	def run(self) -> None :
		print("Starting track segmentation.")
		self.extractData()
		self.buildGraph()
		self.buildSimplifiedGraph()
		self.loadData()
		print("Track segmentation completed Successfully.")

if __name__ == "__main__" :
	parser = argparse.ArgumentParser(description="ETL process for Line Segments dataset")
	parser.add_argument("-i", type=str, required=True, help="Path to the input CSV file")
	parser.add_argument("-o", type=str, default="sumo_data", required=True, help="Path to the output directory")
	parser.add_argument("-sep", type=str, default=";", help="CSV separator (default: ',')")
	args = parser.parse_args()
	track_separator = LineSegmentSeparator(args.i, args.sep, args.o)
	track_separator.run()


