from pyspark.sql import SparkSession

from metadata_etl import MovieMetaDataETL

spark = SparkSession.builder.appName("TrueFilm ETL Test").getOrCreate()

test_dataframe = spark.createDataFrame(
    [
        (10, 20), 
        (50, 200),
        (2, 40)
    ],
    ['revenue', 'budget'] 
)

def test_metadata_etl():
    meta_etl = MovieMetaDataETL(None)
    ratio_df = meta_etl.calculate_rev_to_budget(test_dataframe)
    assert ratio_df

def test_metadata_etl_values():
    meta_etl = MovieMetaDataETL(None)
    ratio_df = meta_etl.calculate_rev_to_budget(test_dataframe)
    ratio_df = ratio_df.toPandas()
    assert ratio_df.rev_ratio.to_list() == [0.5, 0.25, 0.05]

def test_metadata_etl_topn():
    meta_etl = MovieMetaDataETL(None)
    ratio_df = meta_etl.calculate_rev_to_budget(test_dataframe, top_n = 1)
    ratio_df = ratio_df.toPandas()
    assert len(ratio_df == 1)