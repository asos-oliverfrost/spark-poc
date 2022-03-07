import pytest
from split_movie_genre import split_movie_genre
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("poc") \
      .getOrCreate()


def test_split_movie_genre(spark):
    movies_filepath="file:///Users/jawaharp/technical/projects/asos/data/movies/*"
    columns = ["movie_title", "genre"]
    data = [("Toy Story (1995)", "animation"),
            ("Toy Story (1995)", "childrens"),
            ("Toy Story (1995)", "comedy"),
            ("GoldenEye (1995)", "Action"),
            ("GoldenEye (1995)", "Adventure"),
            ("Get Shorty (1995)", "Action"),
            ("Get Shorty (1995)", "comedy"),
            ("Get Shorty (1995)", "drama"),
            ("Copycat (1995)", "crime"),
            ("Copycat (1995)", "drama")]

    rdd = spark.sparkContext.parallelize(data)
    expected_df = rdd.toDF(columns)
    actual_df = split_movie_genre(movies_filepath)
    assert actual_df.limit(10).collect() == expected_df.collect()