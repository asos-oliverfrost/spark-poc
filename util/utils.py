import logging
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("poc") \
    .getOrCreate()

def get_latest_file_path(dir_path):
  """To get the latest directory path
  :param: directory path
  :return: latest directory"""
  latest_dir_dates_to_process = max(dbutils.fs.ls(dir_path))
  logging.info(f"get_latest_file_path:Printing the Latest directiory: {latest_dir_dates_to_process}")
  latest_dir_path=latest_dir_dates_to_process.path
  logging.info(f"get_latest_file_path:Printing the Latest Directory Path:{latest_dir_path}")
  return latest_dir_path

def get_schema(dir_path, fmt ):
    """Provide the schema for directory path
    :param: directory path
    :param: format of the file
    :return: returns the schema"""
    logging.info(f"get_schema:Directory Path:{dir_path} Format of file:{fmt}")
    latest_schema = read(dir_path, fmt ).schema
    return latest_schema

def read(input_path, file_fmt ):
    """This function will read and file and return dataframe
     :param: inputfile Path
     :param: file format
     :param: spark object
     :return: pyspark dataframe """
    df = spark.read.format(file_fmt).option("header", True).option("inferSchema", "true").load(input_path)
    return df