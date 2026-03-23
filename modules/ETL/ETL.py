import pyspark.sql as sql

class ETL :
	def __init__(self, spark: sql.SparkSession, filename : str, separator : str, output_filename : str,
	infer_schema : bool = True) -> None :
		self.spark : sql.SparkSession = spark
		self.filename : str = filename
		self.separator : str = separator
		self.output_filename : str = output_filename
		self.dataframe : sql.DataFrame = None
		self.infer_schema : bool = infer_schema

	def extract(self) -> None :
		"""Extract the data from the source file and load it into a Spark DataFrame."""
		print("Extracting data from source file...")
		self.dataframe = self.spark.read.csv(self.filename, header=True, sep=self.separator, 
		inferSchema=self.infer_schema)
		print("Data extraction completed successfully.")

	def convert(self) -> None :
		"""Convert the data/datatypes in the dataframe."""
		print("Converting data types and values in the dataframe...")


	def rename(self) -> None :
		"""Rename columns in the dataframe."""
		print("Renaming columns in the dataframe...")

	def drop(self) -> None :
		"""Drop columns from the dataframe."""
		print("Dropping columns from the dataframe...")

	def reorder(self) -> None :
		"""Reorder columns in the dataframe."""
		print("Reordering columns in the dataframe...")

	def transform(self) -> None :
		"""Apply all transformations to the dataframe."""
		print("Transforming data...")
		self.convert()
		self.rename()
		self.drop()
		self.reorder()
		print("Data transformation completed.")

	def load(self) -> None :
		"""Load the transformed dataframe to the output file."""
		print("Loading data to output file...")
		self.dataframe.toPandas().to_csv(self.output_filename, index=False, sep=";")
		print("Data loading completed successfully.")

	def run(self) -> None :
		"""Run the entire ETL process."""
		self.extract()
		self.transform()
		self.load()
		print("ETL process completed successfully.")