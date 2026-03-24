from ETL import ETL
import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import argparse

class PlatformsETL(ETL) :
	def __init__(self, spark : sql.SparkSession, filename : str, separator : str , 
	output_filename : str, nb_decimals : int):
		super().__init__(spark, filename, separator, output_filename, infer_schema = False)
		self.nb_decimals : int = nb_decimals

	def convert(self):
		super().convert()
		### Converting the column "Geo Point" ###
		coords : sql.Column = F.split(F.col("Geo Point"), ",")
		self.dataframe = (self.dataframe
			.withColumn("X", F.floor(coords.getItem(0).cast("double") * 1e6) / 1e6) 
			.withColumn("Y", F.floor(coords.getItem(1).cast("double") * 1e6) / 1e6)
		)

		### Converting the columns to the right datatypes ###
		self.dataframe = self.dataframe.withColumn("ID quai", F.col("ID quai").cast("integer"))
		self.dataframe = self.dataframe.withColumn(
			"Nom du point d'arrêt", 
			F.col("Nom du point d'arrêt").cast("string")
		)
		self.dataframe = self.dataframe.withColumn("Numéro du quai", F.col("Numéro du quai").cast("integer"))
		self.dataframe = self.dataframe.withColumn("Platform type", F.col("Platform type").cast("string"))
		self.dataframe = self.dataframe.withColumn("PTCAR ID", F.col("PTCAR ID").cast("integer"))

	def rename(self):
		super().rename()
		self.dataframe = self.dataframe.withColumnRenamed("ID quai", "ID")
		self.dataframe = self.dataframe.withColumnRenamed("Nom du point d'arrêt", "Station_name")
		self.dataframe = self.dataframe.withColumnRenamed("Numéro du quai","Platform_number")
		self.dataframe = self.dataframe.withColumnRenamed("Platform type","Platform_type")
		self.dataframe = self.dataframe.withColumnRenamed("PTCAR ID","Station_ID")

	def drop(self):
		super().drop()
		self.dataframe = self.dataframe.drop(
			"Stopplaats naam",
			"Perrontype",
			"Type de quai",
			"Nominale perronhoogte",
			"Hauteur nominale du quai",
			"Platform nominal height",
			"Type stopplaats",
			"Type de point d'arrêt",
			"Type of stopping point",
			"Area",
			"Arrondissement",
			"Geo Point"
		)

	def reorder(self):
		super().reorder()
		self.dataframe = self.dataframe.select(
			"ID",
			"Station_ID",
			"Station_name",
			"Platform_type",
			"X",
			"Y"
		)

	def run(self) -> None :
		print("Running Station platforms ETL process...")
		super().run()

if __name__ == "__main__" :
	args_parser : argparse.ArgumentParser = argparse.ArgumentParser(description="ETL for station plaforms dataset")
	args_parser.add_argument("-i", type=str, help="Path to the input CSV file")
	args_parser.add_argument("-sep", type=str, default=";", help="Separator used in the input CSV file")
	args_parser.add_argument("-o", type=str, help="Path to the output CSV file")
	args_parser.add_argument("-d", type=int, default=6, help="Number of decimals to keep for Geo Point coordinates")
	args : argparse.Namespace = args_parser.parse_args()
	spark : sql.SparkSession = (sql.SparkSession.builder
		.appName("PlatformsETL")
		.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
		.getOrCreate()
	)
	etl : PlatformsETL = PlatformsETL(spark, args.i, args.sep, args.o, int(args.d))
	etl.run()
	spark.stop()