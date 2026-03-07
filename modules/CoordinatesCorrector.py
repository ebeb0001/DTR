import pandas as pd
import numpy as np
import json
import argparse

class CoordinatesCorrector:
	def __init__(self, input_station_file : str, input_station_to_station_file : str, 
	output_station_file : str, output_station_to_station_file : str) -> None :
		self.input_station_file : str = input_station_file
		self.input_station_to_station_file : str = input_station_to_station_file
		self.output_station_file : str = output_station_file
		self.output_station_to_station_file : str = output_station_to_station_file
		self.stations_df : pd.DataFrame = None
		self.station_to_station_df : pd.DataFrame = None

	def extractData(self) -> None :
		print("Extracting data...")
		self.stations_df : pd.DataFrame = pd.read_csv(self.input_station_file, sep=";")
		self.station_to_station_df : pd.DataFrame = pd.read_csv(self.input_station_to_station_file, sep=";")
		self.station_to_station_df['Coordinates'] = self.station_to_station_df['Coordinates'].apply(self.parseCoordinates)
		print("Data extraction completed successfully.")

	def parseCoordinates(self, coord_str : str) -> None :
		try:
			coords = json.loads(coord_str)
			if isinstance(coords, list) and all(isinstance(c, list) for c in coords):
				return [[float(x) for x in c] for c in coords]
		except Exception:
			print(f"Invalid coordinate format: {coord_str}")
		return np.nan

	def checkStations(self) -> None :
		print("Checking station IDs in station_to_station dataset against stations dataset...")
		stations : pd.DataFrame = pd.concat([
		self.station_to_station_df[["Departure_station_id"]],
		self.station_to_station_df[
				["Arrival_station_id"]
			].rename(columns={"Arrival_station_id": "Departure_station_id"})
		], ignore_index=True).drop_duplicates()
		found_ids : pd.Series[bool] = stations["Departure_station_id"].isin(self.stations_df["ID"])
		found_stations : int = found_ids.sum()
		nb_stations : int = len(stations)
		missing_ids : pd.Series = stations.loc[~found_ids, "Departure_station_id"]
		for station_id in missing_ids:
			print(f"Station ID {station_id} not found in operational points")
		print(f"PTCAR_IDs found: {found_stations} / {nb_stations} ({found_stations / nb_stations:.2%}%)")

	def checkCoordinates(self) -> None :
		print("Checking station coordinates against polyline coordinates...")
		df = self.station_to_station_df.copy()

		# 1) Extraire les points de début/fin depuis la polyline "Coordinates" (liste de [x, y])
		df["start"] = df["Coordinates"].str[0]    # premier point
		df["end"]   = df["Coordinates"].str[-1]   # dernier point

		# Séparer x/y pour start et end (deux colonnes chacune)
		df[["start_x", "start_y"]] = pd.DataFrame(df["start"].tolist(), index=df.index)
		df[["end_x",   "end_y"  ]] = pd.DataFrame(df["end"].tolist(),   index=df.index)

		# 2) Préparer la table des points opérationnels pour jointure
		ops = self.stations_df[["ID", "Geo_x", "Geo_y"]].copy()

		# 3) Joindre les coordonnées de la station de départ (par ID)
		df = df.merge(
			ops.rename(columns={"Geo_x": "dep_x", "Geo_y": "dep_y"}),
			left_on="Departure_station_id", right_on="ID", how="left"
		).drop(columns=["ID"])

		# 4) Joindre les coordonnées de la station d'arrivée (par ID)
		df = df.merge(
			ops.rename(columns={"Geo_x": "arr_x", "Geo_y": "arr_y"}),
			left_on="Arrival_station_id", right_on="ID", how="left"
		).drop(columns=["ID"])

		# 5) Comparer vectoriellement :
		# - Une station (dep ou arr) est valide si ses coords correspondent au start OU au end de la polyline
		dep_matches_start = (df["dep_x"].eq(df["start_x"])) & (df["dep_y"].eq(df["start_y"]))
		dep_matches_end   = (df["dep_x"].eq(df["end_x"]))   & (df["dep_y"].eq(df["end_y"]))
		dep_valid = dep_matches_start | dep_matches_end

		arr_matches_start = (df["arr_x"].eq(df["start_x"])) & (df["arr_y"].eq(df["start_y"]))
		arr_matches_end   = (df["arr_x"].eq(df["end_x"]))   & (df["arr_y"].eq(df["end_y"]))
		arr_valid = arr_matches_start | arr_matches_end

		both_valid = dep_valid & arr_valid

		# 6) Comptages
		nb_total_stations = len(df)
		valid_departure_coords = int(dep_valid.sum())
		valid_arrival_coords   = int(arr_valid.sum())
		valid_both_coords      = int(both_valid.sum())

		print(f"Valid departure coordinates: {valid_departure_coords} / {nb_total_stations} ({valid_departure_coords / nb_total_stations:.2%}%)")
		print(f"Valid arrival coordinates: {valid_arrival_coords} / {nb_total_stations} ({valid_arrival_coords / nb_total_stations:.2%}%)")
		print(f"Valid both coordinates: {valid_both_coords} / {nb_total_stations} ({valid_both_coords / nb_total_stations:.2%}%)")

	def correctStations(self) -> None :
		print("Correcting station coordinates...")
		new_station : pd.DataFrame = pd.DataFrame([{"ID": 864,
			"Geo_x" : 51.182317,
			"Geo_y" : 4.447188,
			"Symbolic_name": "GMOD",
			"Classification" : "Stop in open track",
			"Code_TAF_TAP" : "BE00863",
			"Name_FR_short": "Deurnesteenweg",
			"Name_FR_full": "Mortsel-Deurnesteenweg"}]
		)
		self.stations_df = pd.concat([self.stations_df, new_station], ignore_index=True)
		self.stations_df.loc[self.stations_df["Name_FR_short"] == "Ampsin", "Symbolic_name"] = "AMPS"
		self.stations_df = self.stations_df.dropna()
		self.stations_df["ID"] = self.stations_df["ID"].astype(int) 

	def replace_first(self, coords : list[list[float]]) -> list[list[float]] :
		return [[50.779471, 4.333527]] + coords[1:]

	def replace_last(self, coords : list[list[float]]) -> list[list[float]] :
		return coords[:-1] + [[50.779471, 4.333527]]

	def correctCoordinates(self, mask, first : bool) -> None :
		print(
			f"Correcting {'first' if first else 'last'} coordinates for {mask.sum()} station_to_station records..."
		)
		if first :
			self.station_to_station_df.loc[mask, "Coordinates"] = (
				self.station_to_station_df.loc[mask, "Coordinates"].apply(self.replace_first)
			)
		else :
			self.station_to_station_df.loc[mask, "Coordinates"] = (
				self.station_to_station_df.loc[mask, "Coordinates"].apply(self.replace_last)
			)

	def loadData(self) -> None :
		print("Loading corrected data to output files...")
		self.stations_df.to_csv(self.output_station_file, sep=";", index=False)
		self.station_to_station_df.to_csv(self.output_station_to_station_file, sep=";", index=False)
		print("Data loading completed successfully.")

	def run(self) -> None :
		self.extractData()
		self.checkStations()
		self.checkCoordinates()
		self.correctStations()
		self.checkStations()
		mask_1 : pd.Series[bool] = (
			(
				(self.station_to_station_df["Departure_station_id"] == 837) & 
				(self.station_to_station_df["Arrival_station_id"] == 128)
			) | (
				(self.station_to_station_df["Departure_station_id"] == 128) & 
				(self.station_to_station_df["Arrival_station_id"] == 837)
			)
		)
		mask_2 : pd.Series[bool] = (
			(
				(self.station_to_station_df["Departure_station_id"] == 837) & 
				(self.station_to_station_df["Arrival_station_id"] == 1081)
			) | (
				(self.station_to_station_df["Departure_station_id"] == 1081) & 
				(self.station_to_station_df["Arrival_station_id"] == 837)
			)
		)
		self.correctCoordinates(mask_1, first=True)
		self.correctCoordinates(mask_2, first=False)
		self.checkCoordinates()
		self.loadData()
		print("Coordinate correction process completed successfully.")

if __name__ == "__main__" :
	parser : argparse.ArgumentParser = argparse.ArgumentParser(
		description="Correct station coordinates in the datasets."
	)
	parser.add_argument("-in_st", type=str, required=True, 
	help="Path to the input stations CSV file.")
	parser.add_argument("-in_st_to_st", type=str, required=True, 
	help="Path to the input station_to_station CSV file.")
	parser.add_argument("-out_st", type=str, required=True, 
	help="Path to the output corrected stations CSV file.")
	parser.add_argument("-out_st_to_st", type=str, required=True, 
	help="Path to the output corrected station_to_station CSV file.")
	args : argparse.Namespace = parser.parse_args()

	corrector : CoordinatesCorrector = CoordinatesCorrector(
		input_station_file=args.in_st,
		input_station_to_station_file=args.in_st_to_st,
		output_station_file=args.out_st,
		output_station_to_station_file=args.out_st_to_st
	)
	corrector.run()