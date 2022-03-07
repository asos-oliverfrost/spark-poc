import pytest
from utils import get_schema
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("poc") \
      .getOrCreate()


def test_get_schema(spark):
    """This functions is to test the schema"""
    expected_schema = "StructType(List(StructField(genre_id,IntegerType,true),StructField(genre_name,StringType,true)))"
    tag_file_path="file:///Users/jawaharp/technical/projects/asos/data/tags/*"
    actual_schema = get_schema(tag_file_path, "csv")
    print(actual_schema)
    assert str(actual_schema) == str(expected_schema)

