from ETL import ETL
import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import argparse

class TracksETL(ETL) :
	def __init__(self, spark: sql.SparkSession, filename : str, separator : str, 
	output_filename : str, nb_decimals : int, infer_schema : bool = True) -> None :
		super().__init__(spark, filename, separator, output_filename, infer_schema)
		self.nb_decimals : int = nb_decimals

	def convert(self) -> None :
		super().convert()
		### Converting and Renaming the column "Geo Shape" ###
		cleaned_coordinates : sql.Column = F.regexp_replace(F.col("Geo Shape"), r'(^")|("$)', "")
		cleaned_coordinates = F.regexp_replace(cleaned_coordinates, r'""', '"')
		schema : T.StructType = T.StructType([
			T.StructField("coordinates", T.ArrayType(T.ArrayType(T.DoubleType())), True),
			T.StructField("type", T.StringType(), True),
		])
		self.dataframe = self.dataframe.withColumn("parsed_geo_shape", F.from_json(cleaned_coordinates, schema))
		self.dataframe = self.dataframe.withColumn(
			"Coordinates",
			F.to_json(
				F.transform(
					F.col("parsed_geo_shape.coordinates"),
					lambda coord : F.array(
						F.round(coord.getItem(1).cast("double"), self.nb_decimals),
						F.round(coord.getItem(0).cast("double"), self.nb_decimals),
					)
				)
			)
		)

		### Converting the column "Geo Point" ###
		x = F.split(F.col("geo_point_2d"), ",").getItem(0).cast("double")
		y = F.split(F.col("geo_point_2d"), ",").getItem(1).cast("double")
		self.dataframe = (
			self.dataframe
			.withColumn("Geo_x", F.round(x, self.nb_decimals))
			.withColumn("Geo_y", F.round(y, self.nb_decimals))
		)

		### Converting the column "Distance" ###
		self.dataframe = self.dataframe.withColumn("Distance", F.round(F.col("Distance"), self.nb_decimals))

	def rename(self) -> None :
		super().rename()
		self.dataframe = self.dataframe.withColumnRenamed("Gare de départ (id)", "Departure_station_id")
		self.dataframe = self.dataframe.withColumnRenamed("Gare d'arrivée (id)", "Arrival_station_id")

	def drop(self) -> None :
		super().drop()
		self.dataframe = self.dataframe.drop(
			"parsed_geo_shape", 
			"Geo Shape",
			"geo_point_2d",
			"Gare de départ", 
			"Gare d'arrivée"
		)

	def reorder(self) -> None :
		super().reorder()
		self.dataframe = self.dataframe.select(
			"Departure_station_id",
			"Arrival_station_id",
			"Geo_x",
			"Geo_y",
			"Distance",
			"Coordinates"
		)

	def run(self) -> None :
		print("Running Tracks ETL process...")
		super().run()

if __name__ == "__main__" :
	args_parser : argparse.ArgumentParser = argparse.ArgumentParser(description="ETL for tracks dataset")
	args_parser.add_argument("-i", type=str, required=True, help="Path to the input CSV file")
	args_parser.add_argument("-o", type=str, required=True, help="Path to the output CSV file")
	args_parser.add_argument("-sep", type=str, default=";", help="CSV separator (default: ';')")
	args_parser.add_argument("-d", type=int, default=6, help="Number of decimals for rounding coordinates and distance (default: 6)")
	args : argparse.Namespace = args_parser.parse_args()
	spark : sql.SparkSession = (sql.SparkSession.builder
		.appName("TracksETL")
		.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
		.getOrCreate()
	)
	etl : TracksETL = TracksETL(spark, args.i, args.sep, args.o, args.d)
	etl.run()