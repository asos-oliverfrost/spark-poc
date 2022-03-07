import pytest
from top10films import getTop10Films
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("poc") \
      .getOrCreate()


def test_getTop10Films(spark):
    movies_filepath="file:///Users/jawaharp/technical/projects/asos/data/movies/*"
    rating_filepath="file:///Users/jawaharp/technical/projects/asos/data/rating/*"

    columns = ["movie_title", "toprating", "count_userid"]
    data = [("Pather Panchali (1955)", 4.625, 8),
            ("Close Shave, A (1995)", 4.491071428571429, 112),
            ("Schindler's List (1993)", 4.466442953020135, 298),
            ("Wrong Trousers, The (1993)", 4.466101694915254, 118),
            ("Casablanca (1942)", 4.45679012345679, 243),
            ("Wallace & Gromit: The Best of Aardman Animation (1996)", 4.447761194029851, 67),
            ("Shawshank Redemption, The (1994)", 4.445229681978798, 283),
            ("Rear Window (1954)", 4.3875598086124405, 209),
            ("Usual Suspects, The (1995)", 4.385767790262173, 267),
            ("Star Wars (1977)", 4.3584905660377355, 583)]

    rdd = spark.sparkContext.parallelize(data)
    expected_df = rdd.toDF(columns)
    actual_df = getTop10Films(movies_filepath, rating_filepath)
    assert  actual_df.collect() == expected_df.collect()






