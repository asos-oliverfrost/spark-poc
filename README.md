# PySpark MovieLens Project

Building Data Ingestion and Data Transformaton for Movies Lens Dataset using Pyspark and Databricks Notebook to process the data in a Lamba
Architecture which comprises of both Batch and Streaming.Pipeline is divided into staging layer does the Data Ingestion
and Transformation for processing the analysis on the data.
<hr>

## Table of contents
* [Installation](#Installation)
* [Building and Running this Pyspark Project Locally](#Building and Runnig this Pyspark Project Locally)
* [PySpark Databricks Notebooks](#PySpark Databricks Notebooks)
* [Contact](#Contact)
<hr>

## Installation
Use any IDE like PYCHARM or VSCODE. Pycharm used here.
1. Create a Virtual Environment and Install the packages. 

```
pip install pyspark
```

```
pip install pytest
```
<hr>

## Building and Running this Pyspark Project Locally
 #### Project Structure
```
spark-poc/
 |-- data-ingestion/
 |   |-- process_movie_data.ipynb
 |   |-- process_ratings_data.ipynb
 |   |-- process_tag_data.ipynb
 |-- data-transformation/
 |   |-- split_movie_genre.ipynb
 |   |-- split_movie_genre.py
 |   |-- test_split_movie_genre.py
 |   |-- test_top10films.py
 |   |-- Top10_Films.ipynb
 |   |-- top10films.py
 |-- input_data/
 |   |-- movies
 |   |-- | -- 20220305/
 |   |-- | -- | -- movie.csv/
 |   |-- rating
 |   |-- | -- 20220305/
 |   |-- | -- | -- ratings.csv/
 |   |-- | -- 20220306/
 |   |-- | -- | -- ratings.csv/
 |   |-- tags
 |   |-- | -- 20220305/
 |   |-- | -- | -- genre.csv/
 |-- util/
 |   |-- test_utils.py/
 |   |-- utils.ipynb/
 |   |-- utils.py
```

1. Run the Pytest on this project terminal to see the unit test results.

```
pytest
```
<hr>

## PySpark Databricks Notebooks

#### View the Databricks Notebooks on Github

* Click on "ipynb" files on Github to see the Code and Results already ran.

#### To run Data Ingestion on Databricks.

   ##### Import data into Databricks Workspace.


     1. Use this folder path on DBFS ("dbfs://FileStore/tables/) to upload the input data.

     2. Create folders and Upload input files.
        eg: "/FileStore/tables/asos_data/movies/20220306/movies.csv"
     
   ##### Import Notebooks on Databricks Workspace.


      1. Import the Notebooks from Github Data Ingestion folder and Util folder
         into Databricks Workspace.

      2. "Run All" On the notebooks, Since its streaming, Once more data added to the input data
          automatically it picks ups and Process the new data.

      3. To verify Output, Output folder can be found dbfs.
          eg: "dbfs:/FileStore/tables/asos_delta_std/movies/"

      4. Checkpoint folder will be available on dbfs.
         eg: "dbfs:/FileStore/tables/asos_delta_std/movies_checkpoint"
<hr>

## Contact

Created by @Jawahar - feel free to contact me!