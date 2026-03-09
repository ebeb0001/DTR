from ETL import ETL
import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import argparse

class PunctualityETL(ETL) :
	def __init__(self, spark: sql.SparkSession, filename : str, separator : str, 
	output_filename : str, infer_schema : bool = False) -> None :
		super().__init__(spark, filename, separator, output_filename, infer_schema)

	def convert(self) -> None :
		super().convert()
		### Merge time and date in one column ###
		self.dataframe = self.dataframe.withColumn("PLANNED_DATETIME_ARR", 
			F.concat_ws(" ", 
				F.date_format(F.to_date(F.col("PLANNED_DATE_ARR"), "ddMMMyyyy"), "dd-MM-yyyy").cast("string"), 
				F.col("PLANNED_TIME_ARR")
			)
		)
		self.dataframe = self.dataframe.withColumn("PLANNED_DATETIME_DEP", 
			F.concat_ws(" ", 
				F.date_format(F.to_date(F.col("PLANNED_DATE_DEP"), "ddMMMyyyy"), "dd-MM-yyyy").cast("string"), 
				F.col("PLANNED_TIME_DEP")
			)
		)

		### Changing empty values into Null values ###
		string_columns : list[str] = [
			f.name for f in self.dataframe.schema.fields if isinstance(f.dataType, T.StringType)
		]
		for column in string_columns:
			self.dataframe = self.dataframe.withColumn(
				column, 
				F.when(F.col(column) == "", None).otherwise(F.col(column))
			)

		### Convert to right data types ###
		self.dataframe = self.dataframe.withColumn("TRAIN_NO", F.col("TRAIN_NO").cast("int"))
		self.dataframe = self.dataframe.withColumn("PTCAR_NO", F.col("PTCAR_NO").cast("int"))
		self.dataframe = self.dataframe.withColumn("LINE_NO_DEP", F.col("LINE_NO_DEP").cast("string"))
		self.dataframe = self.dataframe.withColumn("DELAY_ARR", F.col("DELAY_ARR").cast("int"))
		self.dataframe = self.dataframe.withColumn("DELAY_DEP", F.col("DELAY_DEP").cast("int"))
		self.dataframe = self.dataframe.withColumn("CIRC_TYP", F.col("CIRC_TYP").cast("int"))
		self.dataframe = self.dataframe.withColumn("CIRC_TYP", F.col("CIRC_TYP").cast("int"))
		self.dataframe = self.dataframe.withColumn("LINE_NO_ARR", F.col("LINE_NO_ARR").cast("string"))
		self.dataframe = self.dataframe.withColumn(
			"PLANNED_DATETIME_ARR", 
			F.to_timestamp(F.col("PLANNED_DATETIME_ARR"), "dd-MM-yyyy H:mm:ss")
		)
		self.dataframe = self.dataframe.withColumn(
			"PLANNED_DATETIME_DEP", 
			F.to_timestamp(F.col("PLANNED_DATETIME_DEP"), "dd-MM-yyyy H:mm:ss")
		)
		self.dataframe = self.dataframe.withColumn(
			"REAL_DATE_ARR", 
			F.date_format(F.to_date(F.col("REAL_DATE_ARR"), "ddMMMyyyy"), "dd-MM-yyyy")
		)
		self.dataframe = self.dataframe.withColumn(
			"REAL_DATE_DEP", 
			F.date_format(F.to_date(F.col("REAL_DATE_DEP"), "ddMMMyyyy"), "dd-MM-yyyy")
		)
		self.dataframe = self.dataframe.withColumn("TRAIN_SERV", F.col("TRAIN_SERV").cast("string"))
		self.dataframe = self.dataframe.withColumn(
			"RELATION_DIRECTION", 
			F.col("RELATION_DIRECTION").cast("string")
		)

	def rename(self) -> None :
		super().rename()
		self.dataframe = self.dataframe.withColumnRenamed("PTCAR_NO", "STOPPING_PLACE_ID")

	def drop(self) -> None :
		super().drop()
		self.dataframe = self.dataframe.drop(
			"PLANNED_DATE_ARR", 
			"PLANNED_DATE_DEP", 
			"PLANNED_TIME_DEP", 
			"PLANNED_TIME_ARR",
			"PTCAR_LG_NM_NL", 
			"REAL_TIME_ARR", 
			"REAL_TIME_DEP", 
			"DATDEP",
			"THOP1_COD",
			"CIRC_TYP"
		)

	def reorder(self) -> None :
		super().reorder()
		self.dataframe = self.dataframe.select(
			"TRAIN_NO", 
			"RELATION", 
			"TRAIN_SERV", 
			"STOPPING_PLACE_ID",
			"LINE_NO_DEP", 
			"DELAY_ARR", 
			"DELAY_DEP",
			"RELATION_DIRECTION", 
			"LINE_NO_ARR", 
			"REAL_DATE_ARR", 
			"REAL_DATE_DEP", 
			"PLANNED_DATETIME_ARR", 
			"PLANNED_DATETIME_DEP"
		)

	def  run(self) -> None :
		print("Running Punctuality ETL process...")
		super().run()

if __name__ == "__main__" :
	args_parser : argparse.ArgumentParser = argparse.ArgumentParser(description="Punctuality ETL process")
	args_parser.add_argument("-i", type=str, required=True, help="Input file path")
	args_parser.add_argument("-o", type=str, required=True, help="Output file path")
	args_parser.add_argument("-sep", type=str, default=",", help="CSV separator (default: ',')")
	args : argparse.Namespace = args_parser.parse_args()
	spark : sql.SparkSession= (sql.SparkSession.builder
		.appName("PunctualityETL")
		.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
		.getOrCreate()
	)
	etl : PunctualityETL = PunctualityETL(spark, args.i, args.sep, args.o)
	etl.run()