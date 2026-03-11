# Importing the global libraries
import pyspark.sql.types as T
import pyspark.sql as sql
import pyspark.sql.functions as F
import datetime as dt
import subprocess
from copy import deepcopy
import argparse

mapping_stations : dict[int : list[int]] = {
	1155 : [404, 736, 1149, 1189],
	516 : [190, 493, 507, 1230],
	264 : [16, 700],
	650 : [868],
	1419 : [376, 539, 632, 637, 1084],
	524 : [139, 523, 1061],
	269 : [789, 894, 1062],
	2061 : [1048],
	528 : [70, 489, 723, 752, 968],
	913 : [313, 610, 611, 767, 789, 895, 995, 1009],
	533 : [77, 427, 719, 1048, 1154, 1260],
	2199 : [798, 961, 1254],
	921 : [535, 908, 920, 1136],
	794 : [413, 747, 791],
	283 : [604, 606, 649, 1005],
	925 : [31, 267, 421, 901, 1159],
	1055 : [191, 218, 227, 383, 812, 826],
	800 : [363, 1048, 1192, 1224],
	1057 : [221, 363, 1048, 1192, 1979],
	674 : [201, 391, 471, 673, 784, 790, 1048, 1744],
	162 : [139, 151, 1088],
	33 : [368, 648, 916, 1174, 1260],
	164 : [347, 458, 600, 910, 923],
	675 : [217, 376, 764, 1839],
	551 : [205, 342, 550, 1092, 1160],
	1577 : [424, 515, 620, 809, 870],
	42 : [139, 546, 554, 977],
	937 : [153, 252, 739, 855, 936],
	428 : [1067, 2011],
	557 : [22, 361, 402, 530],
	2222 : [436, 554, 924, 1232],
	308 : [82, 515, 620, 1125],
	949 : [210, 931, 1261],
	950 : [210, 1195],
	698 : [560, 649, 868],
	1211 : [455, 682],
	1724 : [139, 320, 335, 447, 449, 455],
	1982 : [75, 272, 287, 896, 1043],
	1089 : [138, 151, 906, 1088],
	456 : [130, 447, 449, 1248],
	76 : [61, 911],
	333 : [126, 435, 480, 589],
	2126 : [37, 203, 715, 738, 911, 1218],
	718 : [9, 507, 553, 715, 1238],
	1110 : [84, 992, 996, 1157],
	731 : [27, 562, 726, 728, 1063, 2011],
	93 : [10, 220, 317, 320, 455, 1212],
	734 : [77, 139, 221, 458, 562, 726, 730, 733, 835, 895, 1125],
	1246 : [84, 277, 375, 726, 974, 996],
	225 : [1768, 2089],
	614 : [25, 118, 262, 1102],
	358 : [726, 728, 1067, 2011],
	745 : [412, 664, 744, 1139],
	233 : [157, 324, 523, 1151],
	2027 : [258, 259, 791],
	618 : [255, 620, 809, 870],
	749 : [130, 138, 748, 1073, 1128, 1265],
	108 : [436, 750, 840, 1666],
	1263 : [212, 743],
	236 : [148, 220, 895, 995, 1031],
	109 : [941],
	1011 : [400, 406, 895, 1009],
	117 : [37, 139, 324, 326, 523, 636, 1061, 1141, 1238],
	885 : [205, 805, 902, 1092],
	1273 : [565, 1231, 1348, 1350],
	506 : [128, 504, 759],
	1148 : [619, 726, 971, 1063],
	638 : [27, 210, 266, 399, 726, 929, 1202],
	1023 : [220, 232, 415, 759, 1021]
}

class RailwaySimulationGenerator :
	def __init__(self, stations_file : str, station_to_station_file : str, punctuality_data_file : str, 
	output_dir : str, train_speed : float, nb_days : int, edge_max_speed : float, start_datetime : str, 
	sim_stations : list[int], spark : sql.SparkSession) :
		self.stations_file : str = stations_file
		self.station_to_station_file : str = station_to_station_file
		self.punctuality_data_file : str = punctuality_data_file
		self.output_dir : str = output_dir
		self.train_speed : float = train_speed
		self.nb_days : int = nb_days
		self.edge_max_speed : float = edge_max_speed
		self.start_datetime : dt.datetime = dt.datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
		self.end_datetime : dt.datetime = self.start_datetime + dt.timedelta(days=nb_days)
		self.sim_stations : list[int] = sim_stations
		self.spark : sql.SparkSession = spark 
		self.stations_df : sql.DataFrame = None
		self.station_to_station_df : sql.DataFrame = None
		self.punctuality_data_df : sql.DataFrame = None
		self.stations_dict : dict[int, set[int]] = {}
		self.edges_dict : dict[int : dict[int : dict[str : float | list[list[float]]]]] = {}
		self.additional_stations : list[int] = []
		self.trips : list[list[dict[str : int]]] = []
		self.filenames = {
			"stations" : f"{self.output_dir}/stations.nod.xml",
			"edges" : f"{self.output_dir}/edges.edg.xml",
			"routes" : f"{self.output_dir}/routes.rou.xml",
			"network" : f"{self.output_dir}/network.net.xml",
			"platforms" : f"{self.output_dir}/platforms.add.xml",
			"schedule" : f"{self.output_dir}/schedule.trips.xml",
			"config" : f"{self.output_dir}/config.sumocfg",
			"trains" : f"{self.output_dir}/trains.add.xml"
		}

	def loadStations(self) :
		print("Loading stations data...")
		self.stations_df = self.spark.read.csv(self.stations_file, header=True, inferSchema=True, sep=";")
		for row in self.stations_df.collect() :
			station_id : int = row["ID"]
			if station_id not in self.stations_dict :
				self.stations_dict[station_id] = {}
			else :
				print(f"Two stations have the same ID {station_id}")
			self.stations_dict[station_id]["name"] = row["Name_FR_full"]
			self.stations_dict[station_id]["x"] = row["Geo_y"]
			self.stations_dict[station_id]["y"] = row["Geo_x"]

	def loadEdges(self) :
		print("Loading station to station data...")
		self.station_to_station_df : sql.DataFrame = self.spark.read.csv(self.station_to_station_file, header=True, 
			inferSchema=True, sep=";")
		schema : T.DataType = T.ArrayType(T.ArrayType(T.DoubleType()))
		self.station_to_station_df = self.station_to_station_df.withColumn(
			"Coordinates",
			F.from_json(F.col("Coordinates"), schema)
		)
		for row in self.station_to_station_df.collect():
			departure_station : int = row['Departure_station_id']
			arrival_station : int = row['Arrival_station_id']
			distance : float = row['Distance']
			shape : list[list[float]] = []
			if arrival_station in self.edges_dict and departure_station in self.edges_dict[arrival_station] :
				shape = deepcopy(self.edges_dict[arrival_station][departure_station]["shape"])
				shape.reverse()
			else :
				for coords in row['Coordinates'] :
					if (coords[1], coords[0]) not in shape :
						shape.append((coords[1], coords[0]))
			if departure_station not in self.edges_dict:
				self.edges_dict[departure_station] = {}
			self.edges_dict[departure_station][arrival_station] = {
				"distance" : distance,
				"shape" : None
			}
			if shape is not None :
				coord : list[float] = shape[0]
				if (coord[0] != self.stations_dict[departure_station]["x"] or coord[1] != self.stations_dict[departure_station]["y"]) :
					shape.reverse()
			self.edges_dict[departure_station][arrival_station]["shape"] = shape

	def loadPunctualityData(self) :
		print("Loading punctuality data...")
		self.punctuality_data_df : sql.DataFrame = self.spark.read.csv(self.punctuality_data_file, header=True, 
			inferSchema=True, sep=";")
		window : sql.Window = (
			sql.Window.partitionBy("TRAIN_NO","REAL_DATE_DEP") 
			.orderBy("PLANNED_DATETIME_DEP")
		)
		self.punctuality_data_df = (
			self.punctuality_data_df
			.withColumn(
				"NEXT_STOPPING_PLACE_ID",
				F.lead("STOPPING_PLACE_ID").over(window)
			)
		)

	def loadData(self) :
		print("Loading data...")
		self.loadStations()
		self.loadEdges()
		self.loadPunctualityData()
		print("Data loaded successfully")

	def filterStations(self) :
		print("Filtering stations...")
		for station in self.sim_stations :
			for s in self.edges_dict[station] :
				if s not in self.sim_stations and s not in self.additional_stations :
					self.additional_stations.append(s)
		self.stations_df = self.stations_df.filter(F.col("ID").isin(self.sim_stations) | 
			F.col("ID").isin(self.additional_stations))

	def filterEdges(self) :
		print("Filtering edges...")
		self.station_to_station_df = self.station_to_station_df.filter(
			(F.col("Departure_station_id").isin(self.sim_stations)) &
			(F.col("Arrival_station_id").isin(self.sim_stations))
		)

	def filtrerPunctualityData(self) :
		print("Filtering punctuality data...")
		self.punctuality_data_df = (self.punctuality_data_df.filter(
			(F.col("PLANNED_DATETIME_DEP") >= F.lit(self.start_datetime.strftime("%Y-%m-%d %H:%M:%S"))) & 
			(F.col("PLANNED_DATETIME_DEP") <= F.lit(self.end_datetime.strftime("%Y-%m-%d %H:%M:%S"))))
			.filter(
				F.col("STOPPING_PLACE_ID").isin(self.sim_stations) |
				F.col("NEXT_STOPPING_PLACE_ID").isin(self.sim_stations)
			).orderBy("TRAIN_NO", "REAL_DATE_DEP")
		)

	def filterData(self) :
		print("Filtering data...")
		self.filterStations()
		self.filterEdges()
		self.filtrerPunctualityData()
		print("Data filtered successfully")

	def generateTrips(self) :
		print("Generating trips...")
		trip_id : tuple[int, dt.datetime] = None	
		trip : list[dict[str : int]] = []
		for row in self.punctuality_data_df.collect() :
			if trip_id is None :
				# print(f"Processing trip number {row['TRAIN_NO']}")
				trip_id = (row["TRAIN_NO"], row["REAL_DATE_DEP"])

			delta : float = (row["PLANNED_DATETIME_DEP"] - self.start_datetime).total_seconds()
			info  : dict[str : int] = {
				"departure_station" : row["STOPPING_PLACE_ID"],
				"arrival_station" : row["NEXT_STOPPING_PLACE_ID"],
				"sumo_time" : int(delta)
			}

			in_mapping_s1, in_mapping_s2 = (info["departure_station"] in mapping_stations, 
			info["arrival_station"] in mapping_stations)
			# print(f"Processing info {info} with mapping {in_mapping_s1} and {in_mapping_s2}")

			if in_mapping_s1 and in_mapping_s2 :
				# print(f"Both stations are in the mapping, trying to find a match")
				found = False
				for s1 in mapping_stations[info["departure_station"]] :
					for s2 in mapping_stations[info["arrival_station"]] :
						if s2 in self.edges_dict[s1] :
							info["departure_station"] = s1
							info["arrival_station"] = s2
							found = True
						break
					if found : break
			elif in_mapping_s1 and not in_mapping_s2 :
				# print(f"Only departure station is in the mapping, trying to find a match")
				for s1 in mapping_stations[info["departure_station"]] :
					if info["arrival_station"] in self.edges_dict[s1] :
						info["departure_station"] = s1
						break
			elif not in_mapping_s1 and in_mapping_s2 :
				# print(f"Only arrival station is in the mapping, trying to find a match")
				for s2 in mapping_stations[info["arrival_station"]] :
					if s2 in self.edges_dict[info["departure_station"]] :
						info["arrival_station"] = s2
						break

			if trip_id == (row["TRAIN_NO"], row["REAL_DATE_DEP"]) :
				# print(f"Adding info {info}")
				trip.append(info)
			else :
				# if len(trip) >= len(self.sim_stations) - 1 :
					# print(f"Finished processing trip {trip}")
					trip.sort(key=lambda x: x["sumo_time"])
					self.trips.append(trip)
					trip = [info]
					trip_id = (row["TRAIN_NO"], row["REAL_DATE_DEP"])

		# if trip is not None and len(trip) >= len(self.sim_stations) - 1 :
		if trip is not None :
			trip.sort(key=lambda x: x["sumo_time"])
			self.trips.append(trip)
		self.trips.sort(key=lambda x: x[0]["sumo_time"])

		len_trips : int = len(self.trips)
		for i in range(len_trips) :
			trip : list[dict] = self.trips[i]
			j : int = 0
			while j < len(trip) :
				if trip[j]["departure_station"] is None or trip[j]["arrival_station"] is None :
					trip.pop(j)
				else :
					j += 1
			t : int = 0
			while t < len(trip) - 1 :
				t1 = trip[t]
				t2 = trip[t + 1]
				if (t1["departure_station"] == t2["departure_station"] or 
				t1["arrival_station"] == t2["arrival_station"] or 
				t1["arrival_station"] != t2["departure_station"]) :
					# trip[t]["sumo_time"] = (t1["sumo_time"] + t2["sumo_time"]) / 2
					trip.pop(t + 1)
				else : 
					t += 1
		self.trips.sort(key=lambda x: x[0]["sumo_time"])
		print("Number of trips generated : ", len(self.trips))

	def writeFile(self, filename : str, content : str) :
		with open(filename, 'w', encoding = "utf-8") as f :
			f.write(content)

	def writeStationsFile(self) :
		print("Writing stations file...")
		stations_str : str = '<?xml version="1.0" encoding="UTF-8"?>\n' + '<nodes>\n'
		for station_id in self.sim_stations + self.additional_stations :
			station : dict[str : str | float] = self.stations_dict[station_id]
			stations_str += f'\t<node id="{station_id}" x="{station["x"]}" y="{station["y"]}" type="priority"/>\n'
		stations_str += '</nodes>'
		self.writeFile(self.filenames["stations"], stations_str)

	def writeEdgesFile(self) :
		print("Writing edges file...")
		edges_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + '<edges>\n'
		for depart in self.sim_stations + self.additional_stations :
			for arrival in self.edges_dict[depart] :
				if arrival in self.sim_stations + self.additional_stations :
					edge : dict[str : float | list[list[float]]] = self.edges_dict[depart][arrival]
					edges_str +=f'\t<edge id="{depart}_{arrival}" from="{depart}" to="{arrival}" priority="2" numLanes="1" length="{edge["distance"] * 1000}" speed="{self.edge_max_speed}" allow="rail" shape="'
					for coords in edge["shape"] :
						edges_str += f'{coords[0]},{coords[1]} '
					edges_str += f'"/>\n'
		edges_str += '</edges>'
		self.writeFile(self.filenames["edges"], edges_str)

	def writePlatformsFile(self) :
		print("Writing platforms file...")
		platforms_str = (
			'<?xml version="1.0" encoding="UTF-8"?>\n' + 
			'<additional>\n'
		)
		for station in self.sim_stations + self.additional_stations :
			for neighbor in self.edges_dict[station] :
				if neighbor in self.sim_stations + self.additional_stations :
					platforms_str += f'\t<trainStop id="{station}->{neighbor}" lane="{station}_{neighbor}_0" name="{self.stations_dict[station]["name"]} to {self.stations_dict[neighbor]["name"]} platform" startPos="80" endPos="300" />\n'
		platforms_str += '</additional>'
		self.writeFile(self.filenames["platforms"], platforms_str)

	def writeNetworkFiles(self) :
		print("Writing network files...")
		self.writeStationsFile()
		self.writeEdgesFile()
		self.writePlatformsFile()
		print("Network files written successfully")

	def generateNetwork(self, launch : bool = True) :
		print("Generating network file...")
		network_command = [
			"netconvert",					
			"--node-files", f'{self.filenames["stations"]}',	
			"--edge-files", f'{self.filenames["edges"]}',	
			"--railway.signal.guess.by-stops", "true",			
			"--output-file", f'{self.filenames["network"]}',		
			"--proj.utm", "true"		
		]
		if launch :
			subprocess.run(network_command, check=True)
			print("Network file generated successfully")
		else :
			print(" ".join(network_command))

	def writeTrainsFile(self) :
		print("Writing trains file...")
		train_str : str = ('<?xml version="1.0" encoding="UTF-8"?>\n' +
			'<additional>\n' + 
			f'\t<vType id="myTrain" vClass="rail" length="80" accel="1.2" decel="1.3" maxSpeed="{self.train_speed}" guiShape="rail"/>\n' +
			'</additional>'
		)
		self.writeFile(self.filenames["trains"], train_str)

	def writeTripsFile(self) :
		print("Writing trips file...")
		trips_str = (
			'<?xml version="1.0" encoding="UTF-8"?>\n' + 
			'<routes>\n'
		)
		trip_id = 0
		for trip in self.trips :
			depart_edge = f'{trip[0]["departure_station"]}_{trip[0]["arrival_station"]}'
			depart_time = trip[0]["sumo_time"]
			arrival_edge = f'{trip[-1]["departure_station"]}_{trip[-1]["arrival_station"]}'
			trips_str +=f'\t<trip id="trip_{trip_id}" depart="{depart_time}" from="{depart_edge}" to="{arrival_edge}" type="myTrain" />\n'
			trip_id += 1
		trips_str += '</routes>'
		self.writeFile(self.filenames["schedule"], trips_str)

	def writeRoutesFile(self) :
		print("Writing routes file...")
		routes_str = (
			'<?xml version="1.0" encoding="UTF-8"?>\n' + 
			'<routes>\n' +
			f'\t<vType id="myTrain" length="80.00" maxSpeed="{self.train_speed}" vClass="rail" guiShape="rail" accel="1.2" decel="1.3"/>\n' 
		)
		trip_cmp = 0

		for trip in self.trips :
			edges = [f"{info['departure_station']}_{info['arrival_station']}" for info in trip]
			routes_str += (
				f'\t<vehicle id="train_{trip_cmp}" type="myTrain" depart="{float(trip[0]["sumo_time"])}">\n' +
				f'\t\t<route edges="{" ".join(edges)}" />\n'
			)
			trip_cmp += 1

			for info in trip[1:] :
				routes_str += (
					f'\t\t<stop trainStop="{info["departure_station"]}->{info["arrival_station"]}" until="{float(info["sumo_time"])}" duration="30"/>\n'
				)
			routes_str += '\t</vehicle>\n'
		routes_str += '</routes>'
		self.writeFile(self.filenames["routes"], routes_str)

	def writeScheduleFiles(self) :
		print("Writing schedule files...")
		self.writeTrainsFile()
		self.writeTripsFile()
		self.writeRoutesFile()
		print("Schedule files written successfully")

	def writeConfigurationFile(self) :
		print("Writing configuration file...")
		sumo_config_str : str = (f'<?xml version="1.0" encoding="UTF-8"?>\n' +
		'<configuration>\n' +
			'\t<input>\n' +
				'\t\t<net-file value="network.net.xml"/>\n' +
				'\t\t<route-files value="routes.rou.xml"/>\n' +
				'\t\t<additional-files value="platforms.add.xml"/>\n' +
			'\t</input>\n' +
			'\t<time>\n' +
				'\t\t<begin value="0"/>\n' +
				f'\t\t<end value="{self.nb_days * 24 * 3600}"/>\n' +
			'\t</time>\n' +
			'\t<report>\n' +
				'\t\t<no-step-log value="true"/>\n' +
			'\t</report>\n' +
			'\t<output>\n' +
				'\t\t<tripinfo-output value="tripinfo.xml"/>\n' +
				'\t\t<stop-output value="stopinfo.xml"/>\n' +
				'\t\t<summary-output value="summary.xml"/>\n' +
			'\t</output>\n' +
		'</configuration>'
		)
		self.writeFile(self.filenames["config"], sumo_config_str)
		print("Configuration file written successfully")

	def startSimulation(self) :
		sumo_command = ["sumo-gui", "-c", self.filenames["config"]]
		print("with gui : ", " ".join(sumo_command))
		sumo_command[0] = "sumo"
		print("without gui : ", " ".join(sumo_command))

	def generateSimulation(self) :
		self.loadData()
		self.filterData()
		self.writeNetworkFiles()
		self.generateNetwork()
		self.generateTrips()
		self.writeScheduleFiles()
		self.writeConfigurationFile()
		self.startSimulation()

if __name__ == "__main__" :
	parser = argparse.ArgumentParser(description="Generate a railway simulation for SUMO based on punctuality data")
	parser.add_argument("--stations_file", type=str, required=True, 
	help="Path to the stations file")
	parser.add_argument("--station_to_station_file", type=str, required=True, 
	help="Path to the station to station file")
	parser.add_argument("--punctuality_data_file", type=str, required=True, 
	help="Path to the punctuality data file")
	parser.add_argument("--output_dir", type=str, required=True, 
	help="Directory where the output files will be saved")
	parser.add_argument("--train_speed", type=float, default=33.33, 
	help="Maximum speed of the trains in km/h")
	parser.add_argument("--nb_days", type=int, default=5, 
	help="Number of days to simulate")
	parser.add_argument("--edge_max_speed", type=float, default=33.33, 
	help="Maximum speed on the edges in km/h")
	parser.add_argument("--start_datetime", type=str, default="2025-01-01 06:00:00", 
	help="Start datetime of the simulation in the format YYYY-MM-DD HH:MM:SS")
	parser.add_argument("--sim_stations", type=int, nargs="+", required=True, 
	help="List of station IDs to include in the simulation")
	args = parser.parse_args()

	spark = (sql.SparkSession.builder
		.appName("RailwaySimulationGenerator")
		.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
		.getOrCreate()
	)
	generator = RailwaySimulationGenerator(
		stations_file=args.stations_file,
		station_to_station_file=args.station_to_station_file,
		punctuality_data_file=args.punctuality_data_file,
		output_dir=args.output_dir,
		train_speed=args.train_speed,
		nb_days=args.nb_days,
		edge_max_speed=args.edge_max_speed,
		start_datetime=args.start_datetime,
		sim_stations=args.sim_stations,
		spark=spark
	)
	generator.generateSimulation()