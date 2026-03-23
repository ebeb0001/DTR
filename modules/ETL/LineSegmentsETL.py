from ETL import ETL
import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import argparse

class LineSegmentsETL(ETL) :
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

		### Converting the column "ID du segment" ###
		self.dataframe = self.dataframe.withColumn("ID du segment", F.col("ID du segment").cast("string"))

	def rename(self) -> None :
		super().rename()
		self.dataframe = self.dataframe.withColumnRenamed("ID du segment", "ID")

	def reorder(self) -> None :
		super().reorder()
		self.dataframe = self.dataframe.select(
			"ID",
			"Coordinates",
		)

	def drop(self) -> None :
		super().drop()
		self.dataframe = self.dataframe.drop(
			"parsed_geo_shape", 
			"Geo Shape",
			"Geo Point",
		)

	def run(self) -> None :
		print("Running Main Line Segments ETL process...")
		super().run()

if __name__ == "__main__" :
	parser = argparse.ArgumentParser(description="ETL process for Line Segments dataset")
	parser.add_argument("-i", type=str, required=True, help="Path to the input CSV file")
	parser.add_argument("-o", type=str, required=True, help="Path to the output CSV file")
	parser.add_argument("-sep", type=str, default=";", help="CSV separator (default: ',')")
	parser.add_argument("-d", type=int, default=5, help="Number of decimals for coordinates (default: 5)")
	args = parser.parse_args()
	spark : sql.SparkSession = (sql.SparkSession.builder
		.appName("LineSegmentsETL")
		.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
		.getOrCreate()
	)
	etl = LineSegmentsETL(spark, args.i, args.sep, args.o, args.d)
	etl.run()
	spark.stop()