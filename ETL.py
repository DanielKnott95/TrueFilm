import pandas as pd
import logging
from pandas import json_normalize

from metadata_etl import MovieMetaDataETL
from wikidata_etl import WikipediaAbstractETL
from utils import setup_logger, create_db_connection

logger = setup_logger(__name__)

db_engine = create_db_connection()

class TrueFilmETL:

    def __init__(self, metdadata_path: str, wikiabstract_path: str):
        self.metdadata_path = metdadata_path
        self.wikiabstract_path = wikiabstract_path

    def run_metadata_etl(self, top_n=1000):
        logger.info("Running MetaData ETL")

        metadata_etl = MovieMetaDataETL(self.metdadata_path)
        movie_metadata = metadata_etl.load_spark_dataframe()
        movie_metadata = metadata_etl.calculate_rev_to_budget(movie_metadata, top_n=1000)
        movie_metadata = metadata_etl.run_get_wiki_url(movie_metadata) 
        self.top_movies = movie_metadata.toPandas()

        logger.info("Metadata ETL Complete")
    
    def run_wikidata_etl(self):
        logger.info("Running Wikipedia ETL")

        wiki_etl = WikipediaAbstractETL('enwiki-latest-abstract.xml')
        self.wiki_data = wiki_etl.parse_data(set(self.top_movies.wiki_url))

        logger.info("Wikipedia ETL Complete")

    def expand_rows(self, row, field):
        df = json_normalize(row[field])
        df['imdb_id'] = row['imdb_id']
        df['rev_ratio'] = row['rev_ratio']
        return df
    
    def get_top_movies(self, top_n=1000):
        self.run_metadata_etl(top_n)
        self.run_wikidata_etl()
        self.wiki_data = self.wiki_data.set_index('url')
        self.top_movies = self.top_movies.set_index('wiki_url')

        logger.info("Joining Metadata and Wikidata")
        final_data = self.top_movies.join(self.wiki_data)
        final_data = final_data.rename_axis('wiki_url').reset_index()
        final_data['genres'] = final_data.genres.apply(lambda x: eval(x))
        final_data['production_companies'] = final_data.production_companies.apply(lambda x: eval(x))

        logger.info("Generating Genre and Production Company supporting tables")
        genres = list(final_data.apply(lambda row: self.expand_rows(row, "genres"), axis=1))
        prod_companies = list(final_data.apply(lambda row: self.expand_rows(row, "production_companies"), axis=1))

        self.genre_table = pd.concat(genres).drop_duplicates(subset = ['imdb_id', "name"])
        self.prod_companies_table = pd.concat(prod_companies).drop_duplicates(subset = ['imdb_id', "name"])


        self.final_table = final_data[['imdb_id', 'title', 'budget', 'revenue', 'vote_average', 'rev_ratio', 'wikipedia_title', 'wiki_url', 'abstract']]


def commit_to_sql(table, name, connection):
    table.to_sql(name=name, con=connection, index=False, if_exists='replace')

if __name__ == "__main__":
    etl_instance = TrueFilmETL(
        metdadata_path = "movies_metadata.csv",
        wikiabstract_path = "enwiki-latest-abstract.xml"
    )
    etl_instance.get_top_movies()
    logger.info("Pushing Tables to Database")
    commit_to_sql(etl_instance.final_table, 'metadata_report', db_engine)
    commit_to_sql(etl_instance.prod_companies_table, 'production_companies', db_engine)
    commit_to_sql(etl_instance.genre_table, 'genres', db_engine)
