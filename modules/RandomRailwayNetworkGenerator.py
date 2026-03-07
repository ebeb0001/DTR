import os
import math
import random
import shutil
import subprocess
from time import time
from xml.etree import ElementTree as ET
from dataclasses import dataclass

@dataclass
class TimetableTrip :
	trip_id : str
	depart : int
	from_edge : str
	to_edge : str
	train_type : str = "myTrain"

class RandomRailwayNetworkGenerator:
	def __init__(self, nb_stations : int = 7, nb_edges : int = None, edges_max_speed : float = 33.33, 
	area_size : float = 10000.0, simulation_start_time : int = 0, simulation_end_time : int = 7200, 
	insertion_rate : int = 250, trips_min_distance : float = 1500, train_speed : float = 55.55, seed = 0) -> None :
		self.nb_stations = nb_stations
		self.nb_edges = (min(nb_edges, self.nb_stations * (self.nb_stations - 1)) 
			if nb_edges is not None else self.nb_stations * (self.nb_stations - 1))
		self.edges_max_speed = edges_max_speed
		self.area_size = area_size
		self.simulation_start_time = simulation_start_time
		self.simulation_end_time = simulation_end_time
		self.insertion_rate = insertion_rate
		self.trips_min_distance = trips_min_distance
		self.network = {}
		self.edges = set()
		self.stations = None
		self.stations_platforms = {}
		self.trips : list[TimetableTrip] = []
		self.seed = seed
		self.pairs = []
		self.train_speed = train_speed
		random.seed(self.seed)

	def getCommand(self, cmd : str) -> str :
		"""
		Checks if a command is available int the System PATH.
		Raises an error if the command is not found.
		"""

		path = shutil.which(cmd)
		if not path:
			raise RuntimeError(f"Command not found: {cmd}. Please add it to your PATH.")
		return path

	def findSumoTool(self, tool_name : str) -> str :
		"""
		Locates a SUMO tool (e.g., randomTrips.py) inside the SUMO_HOME/tools directory.
		Raises an error if SUMO_HOME is not set or if the tool cannot be found.
		"""

		sumo_home = os.environ.get("SUMO_HOME")
		if not sumo_home:
			raise RuntimeError("Environment variable SUMO_HOME is not defined.")
		tools = os.path.join(sumo_home, "tools")
		tool_path = os.path.join(tools, tool_name)
		if not os.path.isfile(tool_path):
			raise RuntimeError(f"{tool_name} not found in {tools}")
		return tool_path

	def writeText(self, path : str, content : str) -> None :
		""""Writes the given text content to a file using UTF-8 encoding."""

		with open(path, 'w', encoding = "utf-8") as f :
			f.write(content)

	def addEdge(self, u : int, v : int, undirected : bool = True) -> None :
		"""
		Adds an undirected edge between two station indices (u, v) if it does not already exist.
		Prevents self-loops and duplicate edges.
		"""

		if u == v:
			return
		if (u, v) in self.edges:
			return
		self.edges.add((u, v))
		self.network.setdefault(u, set()).add(v)
		if undirected :
			self.edges.add((v, u))
			self.network.setdefault(v, set()).add(u)

	def generateStations(self) -> None :
		""""""

		print("Generating stations...")
		nodes = []
		for i in range(self.nb_stations):
			nid = f"s{i + 1}"
			x = round(random.uniform(0, self.area_size), 2)
			y = round(random.uniform(0, self.area_size), 2)
			nodes.append((nid, x, y))
		self.stations = {nid: (x, y) for nid, x, y in nodes}
		print(f"Generated stations.")

	def generateEdges(self, undirected : bool = True) -> None :
		""""""

		print("Generating edges...")
		order = list(range(1, self.nb_stations + 1))
		random.shuffle(order)

		# --- Step 1: Building a random spanning tree ---
		# This ensures that the network is fully connected (no isolated stations)
		for i in range(1, self.nb_stations):
			u = order[i]
			v = random.choice(order[:i])
			self.addEdge(u, v, undirected)

		# --- Step 2: Adding random extra edges until reaching NUM_LINKS ---
		# This increases connectivity and adds redundancy to the graph
		while len(self.edges) < self.nb_edges:
			u = random.randrange(1, self.nb_stations + 1)
			v = random.randrange(1, self.nb_stations + 1)
			self.addEdge(u, v, undirected)

		# --- Step 3: Converting numeric node indices into SUMO node IDs ---
		self.edges = {(f"s{ui}", f"s{vi}") for (ui, vi) in self.edges}
		print(f"Generated edges.")

	def generateStationPlatforms(self) -> None :
		""""""

		print("Generating station platforms...")
		for nid in self.stations :
			connected_stations = sorted([e[1] for e in self.edges if e[0] == nid])
			for i, station in enumerate(connected_stations) :
				if nid not in self.stations_platforms :
					self.stations_platforms[nid] = {}
				self.stations_platforms[nid][station] =  i + 1
		# for nid in self.stations_platforms :
		# 	print(f"Station {nid} platforms: {self.stations_platforms[nid]}")
		print("Generated station platforms.")

	def writeStationsFile(self, output_dir : str = "test") -> None :
		""""""

		print("Writing stations file...")
		stations_str = (
			'<?xml version="1.0" encoding="UTF-8"?>\n' + 
			'<nodes>\n' +
			"".join(f'\t<node id="{nid}" x="{self.stations[nid][0]}" y="{self.stations[nid][1]}" type="priority"/>\n' for nid in self.stations) +
			'</nodes>'
		)
		self.writeText(os.path.join(output_dir, "stations.nod.xml"), stations_str)
		print("Stations file written.")

	def writeEdgesFile(self, output_dir : str = "test") -> None :
		""""""

		print("Writing edges file...")
		edges_str = (
			'<?xml version="1.0" encoding="UTF-8"?>\n'
			'<edges>\n' +
			"".join(
				f'\t<edge id="{a}_{b}" from="{a}" to="{b}" priority="2" numLanes="1" length="{random.randint(1500, 20000)}" speed="{self.edges_max_speed}" allow="rail"/>\n'
				for a, b in self.edges
			) +
			'</edges>'
		)
		self.writeText(os.path.join(output_dir, "edges.edg.xml"), edges_str)
		print("Edges file written.")

	def writeStationsPlatformsFile(self, output_dir : str = "test") -> None :
		""""""

		print("Writing station platforms file...")
		platforms_str = (
			'<?xml version="1.0" encoding="UTF-8"?>\n' + 
			'<additional>\n' 
		)
		for nid in self.stations_platforms :
			for dest in self.stations_platforms[nid] :
				platform = self.stations_platforms[nid][dest]
				platforms_str +=f'\t<trainStop id="{nid}_platform_{platform}" lane="{nid}_{dest}_0" name="{nid} platform {platform}" startPos="80" endPos="300" />\n'
		platforms_str += '</additional>'

		self.writeText(os.path.join(output_dir, "station_platforms.add.xml"), platforms_str)
		print("Station platforms file written.")

	def writeTrainTypesFile(self, output_dir : str = "test") -> None :
		""""""

		print("Writing vehicle types file...")
		train_str =('<?xml version="1.0" encoding="UTF-8"?>\n' +
		'<additional>' + 
			f'\t\<vType id="myTrain" vClass="rail" length="80" accel="0.5" decel="1.0" maxSpeed="{self.train_speed}" guiShape="rail"/>\n' +
		'</additional>')
		self.writeText(os.path.join(output_dir, "trains.add.xml"), train_str)
		print("Vehicle types file written.")

	def writeNetworkFiles(self, output_dir : str = "test") -> None :
		""""""

		print("Writing network files...")
		self.writeStationsFile(output_dir)
		self.writeEdgesFile(output_dir)
		self.writeStationsPlatformsFile(output_dir)
		self.writeTrainTypesFile(output_dir)
		print("Network files written.")

	def writeScheduleTripsFile(self, output_dir : str = "test") -> None :
		""""""

		print("Writing timetable file...")
		trips_str = (
			'<?xml version="1.0" encoding="UTF-8"?>\n' + 
			'<routes>\n' 
		)
		for trip in self.trips :
			trips_str +=f'\t<trip id="{trip.trip_id}" depart="{trip.depart}" from="{trip.from_edge}" to="{trip.to_edge}" type="{trip.train_type}" />\n'
		trips_str += '</routes>'
		self.writeText(os.path.join(output_dir, "schedule.trips.xml"), trips_str)
		print("timetable file written.")

	def generateNetwork(self, utm : bool = False, output_dir : str = "test") -> None :
		""""""

		print("Generating SUMO network...")
		cmd = [
			self.getCommand("netconvert"),					# ensure netconvert exists in the system PATH
			"--node-files", f"{output_dir}/stations.nod.xml",		# input: node definitions
			"--edge-files", f"{output_dir}/edges.edg.xml",		# input: edge definitions
			"--railway.signal.guess.by-stops", "true",			# enable automatic signal placement based on train stops
			"--output-file", f"{output_dir}/network.net.xml",		# output: compiled SUMO network
			"--proj.utm", f"{utm}",		# output: compiled SUMO network
		]
		print(">>", " ".join(cmd))
		subprocess.run(cmd, check=True)
		print("SUMO network generated.")

	def buildPairsFromEdges(self, nb_pairs = 7) -> list[tuple[int]] :
		""""""

		rng = random.Random(self.seed)
		all_edges = [f"{a}_{b}" for (a, b) in self.edges]  # uses your network edges
		rng.shuffle(all_edges)

		od_pairs = []
		used = set()
		for i in range(0, len(all_edges), 2):
			f = all_edges[i]
			t = all_edges[(i + 1) % len(all_edges)]
			if f != t and (f, t) not in used:
				od_pairs.append((f, t))
				used.add((f, t))
			if len(od_pairs) >= nb_pairs:
				break
		return od_pairs

	def makePeriodicTrips(self, pairs, headway = 1000, start_offsets = None) -> None:
		""""""

		def get_depart(trip : TimetableTrip) -> float:
			depart = trip.depart
			return float(depart) if depart is not None else -1.

		k = 0
		if start_offsets is None:
			start_offsets = [0] * len(pairs)

		for (from_edge, to_edge), offset in zip(pairs, start_offsets):
			t = offset
			while t <= self.simulation_end_time - 5000:
				k += 1
				self.trips.append(TimetableTrip(
					trip_id=f"TT{k}",
					depart=t,
					from_edge=from_edge,
					to_edge=to_edge
				))
				t += headway
		sorted_trips = sorted(self.trips, key=get_depart)
		self.trips = sorted_trips

	def generateTrips(self, output_dir : str = "test") -> None :
		""""""

		print("Generating trips...")
		cmd = [
			self.getCommand("python"),								# ensure Python is available
			self.findSumoTool("randomTrips.py"),				# locate randomTrips.py inside SUMO_HOME/tools
			"-n", f"{output_dir}/network.net.xml",				# input: the compiled SUMO network
			"-o", f"{output_dir}/schedule.trips.xml",			# output: generated trips file
			"-a", f"{output_dir}/trains.add.xml",				# use the defined vehicle type(s)
			"--trip-attributes", 'type="myTrain"',				# assign the vType 'myTrain' to each trip
			"--edge-permission", "rail",						# ensure only rail edges are used
			"-b", str(self.simulation_start_time),				# begin time for trip generation
			"-e", str(self.simulation_end_time // 2),			# end time for trip generation
			"--insertion-rate", str(self.insertion_rate),		# train insertion rate (s)	
			"--min-distance", str(self.trips_min_distance),		# minimum origin-destination distance
		]
		print(">>", " ".join(cmd))
		subprocess.run(cmd, check=True)

	def generateRoutes(self, output_dir : str = "test") -> None :
		""""""

		print("Generating routes...")
		cmd = [
			self.getCommand("duarouter"),						# ensure duarouter is available
			"--net-file", f"{output_dir}/network.net.xml",			# input: compiled network
			"--route-files", f"{output_dir}/schedule.trips.xml",		# input: generated trips file
			"--additional-files", f"{output_dir}/trains.add.xml",			# input: additional file with vehicle types
			"--output-file", f"{output_dir}/routes.rou.xml",			# output: generated routes file
			"--ignore-errors", "true",						# stop if any routing error occurs
		]
		print(">>", " ".join(cmd))
		subprocess.run(cmd, check=True)

	def addStops(self, output_dir : str = "test") -> None :
		""""""

		print("Adding stops to routes...")
		routes_path = os.path.join(output_dir, "routes.rou.xml")
		routes_tree = ET.parse(routes_path)
		routes_root = routes_tree.getroot()
		edges_path = os.path.join(output_dir, "edges.edg.xml")
		edges_tree = ET.parse(edges_path)
		edges_root = edges_tree.getroot()
		edges = edges_root.findall("edge")

		# for train in routes_root.findall('vehicle') :
		# 	route_edges = train.find('route').get('edges').split()
		# 	for edge in route_edges :
		# 		start, end = edge.split('_')
		# 		platform = self.stations_platforms[start][end]
		# 		ET.SubElement(train, "stop", trainStop = f"{start}_platform_{platform}", duration = "60")

		for train in routes_root.findall('vehicle') :
			t_dep_prev = float(train.get("depart"))
			route_edges = train.find('route').get('edges').split()
			first_station = route_edges[0].split('_')[0]
			for i, edge in enumerate(route_edges) :
				start, end = edge.split('_')
				platform = self.stations_platforms[start][end]
				travel = .0
				for e in edges :
					if e.get("id") == edge :
						v = min(float(e.get("speed")), self.train_speed)
						travel += float(e.get("length")) // v
				t_arrival = t_dep_prev + travel
				dwell = 60
				t_departure = float(train.get("depart")) + dwell if start == first_station and i == 0 else t_arrival + dwell
				if start == first_station and i == 0 :
					ET.SubElement(
						train, "stop", 
						trainStop = f"{start}_platform_{platform}", 
						until = str(t_departure)
					)
				else :
					ET.SubElement(
						train, "stop", 
						trainStop = f"{start}_platform_{platform}", 
						# duration = str(dwell),
						until = str(t_departure)
					)
				t_dep_prev = t_departure
		routes_tree.write(routes_path, encoding="utf-8", xml_declaration=True)

	def writeSimConfigFile(self, output_dir : str = "test") -> None :
		print("Writing SUMO configuration file...")
		sumo_config_str = f"""<?xml version="1.0" encoding="UTF-8"?>
		<configuration>
			<input>
				<net-file value="network.net.xml"/>
				<route-files value="routes.rou.xml"/>
				<additional-files value="station_platforms.add.xml"/>
			</input>
			<time>
				<begin value="{self.simulation_start_time}"/>
				<end value="{self.simulation_end_time}"/>
			</time>
			<report>
				<no-step-log value="true"/>
			</report>
			<output>
				<tripinfo-output value="tripinfo.xml"/>
				<stop-output value="stopinfo.xml"/>
				<summary-output value="summary.xml"/>
			</output>
		</configuration>"""
		self.writeText(os.path.join(output_dir, "config.sumocfg"), sumo_config_str)
		print("SUMO configuration file written.")