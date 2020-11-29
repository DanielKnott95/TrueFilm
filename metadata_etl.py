import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from SPARQLWrapper import SPARQLWrapper, JSON
from pyspark.sql.functions import udf, desc

from utils import setup_logger

logger = setup_logger(__name__)

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

spark = SparkSession.builder.appName("TrueFilm ETL").getOrCreate()

class MovieMetaDataETL:

    def __init__(self, metadata_path: str):
        self.metadata_path = metadata_path

    def run_movie_etl(self):
        movie_metadata = self.load_spark_dataframe()
        movie_metadata = self.calculate_rev_to_budget(movie_metadata, top_n=1000)
        movie_metadata = self.run_get_wiki_url(movie_metadata) 
        return movie_metadata

    def load_spark_dataframe(self):
        logger.info(f"Loading Spark DataFrame from {self.metadata_path}")
        movie_metadata = spark.read.option("header",True).format("csv").load(self.metadata_path)
        movie_metadata = movie_metadata.filter((movie_metadata.adult == 'False') | (movie_metadata.adult == 'True'))
        movie_metadata = movie_metadata.where("budget>0")
        return movie_metadata
    
    def calculate_rev_to_budget(self, dataframe, top_n = 1000):
        dataframe = dataframe.withColumn("rev_ratio", (F.col("revenue") / F.col("budget")))
        dataframe = dataframe.sort(desc("rev_ratio"))
        if top_n:
            dataframe = dataframe.limit(1000)
        return dataframe

    def run_get_wiki_url(self, dataframe):

        def get_wiki_url(imdbID):
            if not imdbID:
                return ""
            queryString = """
                SELECT ?wppage WHERE {
                ?subject wdt:P345 'IMDB-ID' . 
                ?wppage schema:about ?subject .
                FILTER(contains(str(?wppage),'//en.wikipedia'))
                }
            """

            queryString = queryString.replace("IMDB-ID",str(imdbID))
            sparql.setQuery(queryString)
            sparql.setReturnFormat(JSON)
            
            try:
                results = sparql.query().convert()
                for result in results["results"]["bindings"]:
                    wppage = result["wppage"]["value"]
                return wppage
            except Exception:
                return ""

        udf_get_wiki_url = udf(get_wiki_url, StringType())
        dataframe_wiki_url = dataframe.withColumn("wiki_url", udf_get_wiki_url("imdb_id"))
        return dataframe_wiki_url