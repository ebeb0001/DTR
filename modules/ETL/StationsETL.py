from ETL import ETL
import pyspark.sql as sql
import pyspark.sql.functions as F
import argparse

class StationsETL(ETL) :
	def __init__(self, spark: sql.SparkSession, filename : str, separator : str, 
	output_filename : str, nb_decimals : int, infer_schema : bool = True) -> None :
		super().__init__(spark, filename, separator, output_filename, infer_schema)
		self.nb_decimals : int = nb_decimals

	def convert(self) -> None :
		super().convert()
		### Converting the column "Geo Point" ###
		x = F.split(F.col("Geo Point"), ",").getItem(0).cast("double")
		y = F.split(F.col("Geo Point"), ",").getItem(1).cast("double")
		self.dataframe = (
			self.dataframe
			.withColumn("Geo_x", F.round(x, self.nb_decimals))
			.withColumn("Geo_y", F.round(y, self.nb_decimals))
		)

		### Converting the column "PTCAR ID" ###
		self.dataframe = self.dataframe.withColumn("ID", F.col("PTCAR ID").cast("int"))

	def rename(self) -> None :
		super().rename()
		self.dataframe = self.dataframe.withColumnRenamed("Code TAF/TAP", "Code_TAF_TAP")
		self.dataframe = self.dataframe.withColumnRenamed("Nom symbolique", "Symbolic_name")
		self.dataframe = self.dataframe.withColumnRenamed("Nom FR court","Name_FR_short")
		self.dataframe = self.dataframe.withColumnRenamed("Nom NL court","Name_NL_short")
		self.dataframe = self.dataframe.withColumnRenamed("Nom FR complet","Name_FR_full")
		self.dataframe = self.dataframe.withColumnRenamed("Nom NL complet", "Name_NL_full")
		self.dataframe = self.dataframe.withColumnRenamed("Classification EN", "Classification")

	def drop(self) -> None :
		super().drop()
		self.dataframe = self.dataframe.drop(
			"Geo Point", 
			"Geo shape", 
			"PTCAR ID", 
			"Abréviation LST FR courte", 
			"Abréviation LST NL courte", 
			"Abréviation LST FR complète", 
			"Abréviation LST NL complète", 
			"Nom FR moyen", "Nom NL moyen", 
			"Classification NL", 
			"Classification FR"
		)

	def reorder(self) -> None :
		super().reorder()
		self.dataframe = self.dataframe.select(
			"ID",
			"Geo_x",
			"Geo_y",
			"Code_TAF_TAP",
			"Classification",
			"Symbolic_name",
			"Name_FR_short",
			"Name_FR_full",
		)

	def  run(self) -> None :
		print("Running Stations ETL process...")
		super().run()


if __name__ == "__main__" :
	args_parser : argparse.ArgumentParser = argparse.ArgumentParser(description="ETL for stations dataset")
	args_parser.add_argument("-i", type=str, help="Path to the input CSV file")
	args_parser.add_argument("-sep", type=str, default=";", help="Separator used in the input CSV file")
	args_parser.add_argument("-o", type=str, help="Path to the output CSV file")
	args_parser.add_argument("-d", type=int, default=6, help="Number of decimals to keep for Geo Point coordinates")
	args : argparse.Namespace = args_parser.parse_args()
	spark : sql.SparkSession = (sql.SparkSession.builder
		.appName("StationsETL")
		.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
		.getOrCreate()
	)
	etl : StationsETL = StationsETL(spark, args.i, args.sep, args.o, int(args.d))
	etl.run()
	spark.stop()