import pandas as pd
import numpy as np
import json

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
		self.tracks : pd.DataFrame = None 
		self.switches : pd.DataFrame = None
		self.graph : dict[tuple[float, float, float], set[tuple[float, float, float]]] = dict()
		self.simplified_graph : dict[tuple[float, float, float], set[tuple[float, float, float]]] = dict()

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

	def build_graph(self) -> None :
		print("Building graph...")
		for _, row in self.dataframe.iterrows():
			coords = row["Coordinates"]
			if isinstance(coords, list) and len(coords) > 1:
				norm_coords = [self.normalize_point(tuple(c)) for c in coords]
				for i in range(len(norm_coords) - 1):
					p1 = norm_coords[i]
					p2 = norm_coords[i + 1]
					if p1 != p2:
						self.graph.setdefault(p1, set()).add(p2)
						self.graph.setdefault(p2, set()).add(p1)
		print("Graph building completed successfully.")

	def edgeKey(self, p1 : tuple[float, float, float], 
	p2 : tuple[float, float, float]) -> tuple[tuple[float, float, float], tuple[float, float, float]] :
		return frozenset((p1, p2))
		# return (p1, p2) if p1 < p2 else (p2, p1)

	def contract_2_degree_nodes(self) -> dict[str, list[dict[str :  list[tuple[float, float, float]]
	| tuple[float, float, float]]]] :
		print("Contracting degree-2 nodes...")
		degrees : dict[tuple[float, float, float] : int] = {node: len(neighbors) 
		for node, neighbors in self.graph.items()}
		topological_nodes : set[tuple[float, float, float]] = {node for node, degree 
		in degrees.items() if degree != 2}
		contracted_edges : list[tuple[float, float, float]] = []
		visited_half_edges = set()
		for start in topological_nodes : 
			for neighbor in self.graph[start] :
				half_edge_key = self.edgeKey(start, neighbor)
				if half_edge_key in visited_half_edges :
					continue
				path : list[tuple[float, float, float]] = [start]
				previous : tuple[float, float, float] = start
				current : tuple[float, float, float] = neighbor
				visited_half_edges.add(half_edge_key)
				while True :
					path.append(current)
					if current in topological_nodes :
						break
					neighbors = list(self.graph[current])
					if len(neighbors) != 2 :
						break
					next_node = neighbors[0] if neighbors[1] == previous else neighbors[1]
					half_edge_key = self.edgeKey(current, neighbor)
					if half_edge_key in visited_half_edges :
						break
					visited_half_edges.add(half_edge_key)
					previous, current = current, next_node
				if len(path) > 1 :
					contracted_edges.append({
						"start": path[0],
						"end": path[-1],
						"path": path
					})
		print("Degree-2 node contraction completed successfully.")
		return contracted_edges

	def buildCycles(self) -> list[list[tuple[float, float, float]]] :
		print("Building cycles...")
		degrees : dict[tuple[float, float, float] : int] = {node: len(neighbors) 
		for node, neighbors in self.graph.items()}
		degree_2_nodes : set[tuple[float, float, float]] = {node for node, degree
		in degrees.items() if degree == 2}
		cycles : list[list[tuple[float, float, float]]] = []
		visited_cycle_nodes = set()
		for node in degree_2_nodes :
			if node in visited_cycle_nodes :
				continue
			component : list[tuple[float, float, float]] = []
			stack : list[tuple[float, float, float]] = [node]
			local_visited = set()
			while stack :
				current = stack.pop()
				if current in local_visited :
					continue
				local_visited.add(current)
				component.append(current)
				for neighbor in self.graph[current] :
					if neighbor in degree_2_nodes and neighbor not in local_visited :
						stack.append(neighbor)
			component_set = set(component)
			is_pure_cycle = all(
				len(self.graph[n]) == 2  and 
				all(v in component_set for v in self.graph[n]) for n in component
			)
			if is_pure_cycle :
				ordered_cycle = []
				start = component[0]
				# neighbors = list(self.graph[start])
				previous = None
				current = start
				while True :
					ordered_cycle.append(current)
					visited_cycle_nodes.add(current)
					next_nodes = [n for n in self.graph[current] if n != previous]
					if not next_nodes :
						break
					previous, current = current, next_nodes[0]
					if current == start :
						break
					if current in visited_cycle_nodes :
						break
				cycles.append(ordered_cycle)
		print("Cycle building completed successfully.")
		return cycles

	def buildSimplifiedGraph(self) -> None :
		print("Building simplified graph...")
		contracted_edges : dict[tuple[float, float, float] : int] = self.contract_2_degree_nodes()
		cycles : list[list[tuple[float, float, float]]] = self.buildCycles()
		for segment in contracted_edges:
			a = segment["start"]
			b = segment["end"]

			if a != b:
				self.simplified_graph[a].add(b)
				self.simplified_graph[b].add(a)
		for cycle in cycles:
			for i in range(len(cycle) - 1):
				a = cycle[i]
				b = cycle[i + 1]
				if a != b:
					self.simplified_graph[a].add(b)
					self.simplified_graph[b].add(a)
		print("Simplified graph building completed successfully.")

	def loadSwitches(self) -> None :	
		print()
		node_id : int = 0
		self.switches = pd.DataFrame(columns=["id", "X", "Y", "Elevation"])
		for node in self.simplified_graph :
			self.switches.loc[node_id] = [node_id, node[0], node[1], node[2]]
			node_id += 1
		self.switches.to_csv(self.output_filenames["switches"], index=False)

	def loadTracks(self) -> None :
		track_id : int = 0
		self.tracks = pd.DataFrame(columns=["id", "Departure_switch", "Arrival_switch"])
		for node in self.simplified_graph :
			for neighbor in self.simplified_graph[node] :
				depart_switch_id = self.switches[
				self.switches["X"] == node[0]][self.switches["Y"] == node[1]].iloc[0]["id"]
				arrival_switch_id = self.switches[
				self.switches["X"] == neighbor[0]][self.switches["Y"] == neighbor[1]].iloc[0]["id"]
				self.tracks.loc[track_id] = [track_id, depart_switch_id, arrival_switch_id]
				track_id += 1
		self.tracks.to_csv(self.output_filenames["tracks"], index=False)

	def loadData(self) -> None :
		print("")
		self.loadSwitches()
		self.loadTracks()



