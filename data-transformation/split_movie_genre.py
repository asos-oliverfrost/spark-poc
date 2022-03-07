from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, explode, struct, lit


def to_long(df, by):
    """Filter dtypes and split into column names and type description
    :param: dataframe
    :param: by which column
    :return: dataframe split into rows"""
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = explode(array([struct(lit(c).alias("key"), col(c).alias("val")) for c in cols])).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])


def split_movie_genre(movies_filepath):
    """This function will return dataframe of split by movie genre from the movielens dataset
    :param: movie_filepath dataset of movies
    :param: tag_filepath rating dataset
    :return: dataframe split by movie genre"""

    spark = SparkSession.builder \
      .master("local") \
      .appName("poc") \
      .getOrCreate()

    df_movies = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", "true") \
        .load(movies_filepath)

    cols = ['movie_title', 'unknown', 'Action', 'Adventure', 'animation',
            'childrens', 'comedy', 'crime', 'documentary', 'drama', 'fantasy',
            'filmnoir', 'horror', 'musical', 'mystery']
    df_movies_selected_cols = df_movies.select(*cols)

    df_transposed = to_long(df_movies_selected_cols, ['movie_title'])
    df_final = df_transposed.select("movie_title", col("key").alias("genre")) \
        .where(df_transposed['val'] == 1)
    return df_final
