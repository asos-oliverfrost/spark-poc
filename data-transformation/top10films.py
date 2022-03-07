from pyspark.sql import SparkSession
from pyspark.sql.functions import count, mean, desc


def getTop10Films(movies_filepath, rating_filepath):
    """This function will return Top 10 films from the movielens dataset
    :param: movie_filepath dataset of movies
    :param: rating_filepath rating dataset
    :return: dataframe of top 10 films"""

    spark = SparkSession.builder \
      .master("local") \
      .appName("poc") \
      .getOrCreate()

    df_rating = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", "true") \
        .load(rating_filepath)

    df_movies = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", "true") \
        .load(movies_filepath)

    cols_select = ["user_id", "movie_id", "movie_title", "rating"]
    df_joined = df_rating.join(df_movies,"movie_id").select(*cols_select)

    df_final = df_joined.groupBy("movie_title") \
        .agg(mean("rating").alias("toprating"), count("user_id").alias("count_userid")) \
        .orderBy(desc("toprating"))

    df_final = df_final.where(df_final['count_userid'] >= 5).limit(10)

    return df_final
