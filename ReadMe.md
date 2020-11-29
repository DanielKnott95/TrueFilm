# TrueFilm Data Engineer Project

This project aims to provide an data pipeline for an open source imdb movies metadata dataset, to extract, transform and load the data into a postgres database.
The data can then be queried to gain insight into the top performing films, with success being measure by the ratio of revenue to budget for a particular film.
The films themselves can be queried or can be aggregated at the genre and production company level.

## Running the pipeline
The solution is entirely containerised using [Docker](https://www.docker.com/products/docker-desktop), so in order to run the pipeline docker must be installed on the host machine. However this is the only prerequisite. 

The pipeline consists of five stages outlined below:
1. System Boot
2. Data Download
3. Run the tests
4. Run the Extract Transform Load Pipeline
5. Deploy the demo analytics dashboard to walk through sample use cases

Each of these stages will be discussed below.
All commands should be executed in the repository directory.

## System Boot
Once the repository  has been cloned and docker installed the system can be booted up with the following command

```bash
docker-compose up
```
This will spin up a docker stack compose of two containers:
1. The `backend` container, housing the ETL logic code, tests and dashboard.
2. The `postgres` container, for persistant data storage.

## Data Download
Once the docker stack has been instantiated, the supporting wikipedia data can be downloaded using the following command to execute the data download shell script within the container:
```bash
docker exec -it backend chmod +x data_download.sh && ./data_download.sh
```
This will pull the latest wikipedia abstract data dump, used during the ETL process.

## Run the tests
With all the data collected, small tests can be run to ensure that the main ETL calculation code is working as expected and to test for correctness. The tests are executed using the pytest library.
To execute the tests run the following command:
```bash
docker exec -it backend python -m pytest -p no:warnings
```
Hopefully all tests will pass and the main ETL pipeline can be run.

## Run the Extract Transform Load Pipeline
With all data downloaded the main ETL pipeline is executed using the following command.
```bash
docker exec -it backend python ETL.py
```
The Pipeline has three main stages:
1. MetaData ETL

    The main IMDB metadata csv is read and processed using PySpark.
    Although the data included in this example repository is in the order of Megabytes, by building the pipeline in PySpark the process would be able to be scaled to much larger datasets, both in terms of length and width, so that as the solution scaled up distributed processing could be leveraged and processing times kept down.

    In order to be able to join on the Wikipedia Abstract data, the Wikipedia Article URL must be found. I experimented to see if it was possible to match the IMDB data to the Wikipedia abstract data just by using the IMDB id, however the quality of IMDB links in the Wikipedia abstract data seemed fairly poor. To mitigate this issue, the ETL process queries the external Wikidata database using SPARQL. By querying this database we can match every Movie in the dataset to its official Wikipedia page using the imdbID. Ideally this process would not call for a query over the network as this introduced a bottleneck to the Pipeline, but in future this could be mitigated by deployed the solution on cloud with fast network speeds, as well as using spark (or a lambda function) to parallelize the querying.
    
    Each films revenue to budget ratio is calculated, and the top films are taken. The default value of the Pipeline is to take the top 1000 films but this can be customised.
    
2. Wikidata ETL

    The Wikidata ETL process parses the large XML Wikipedia abstract dump.
    This dataset contains the title, url, abstract as well as any links included on every English wikipedia page. All we need from this dataset is the title, url and abstract. We will join on the url to the IMDB data.
    The Wikidata ETL process uses a memory efficient parsing system utilising python's `iterparse` function, whereby rather than loading the entire dataset into memory at once (which would likely crash any default docker container), it loads individual xml elements iteratively and deals with them in turn. 
    An additional memory saving tactic is employed by only storing in memory the xml elements which have a URL match with the IMDB movies processed in step 1. This match is used by checking if an elements URL is in a python set of the IMDB wikipedia urls. Since sets in python are stored as hash tables this is a very efficient lookup and barely effects computational time whilst massively reducing memory usage.
    
3. Main ETL

    With the IMDB and Wikipedia data transformed, the two datasets are joined together.
    The genres and production companies information is stored as nested lists within the dataset, using Pandas these are extracted and transformed into the respective supporting tables.
    The three tables, Metadata, Genres and Production Companies are then pushed to the postgres database, and the ETL pipeline finishes.

## Deploy the demo analytics dashboard to walk through sample use cases
With the data transformed and loaded into the database, we can query the data to provide insights into the top performing films. A sample dashboard is included with this repository as a guide on how to query and use the database. In order to view this dashboard / guide the following command should be run:
```bash
docker exec -it backend streamlit run insights_dashboard.py
```
Then, navigate to http://localhost:8502/ on your host machine to view the dashboard.

To independently query the data you can connect to the database on your host machine using the postgres client of your preference with the following information:
host: localhost
database: postgres
user: postgres
password: password _(this should be changed on deployment)_
port: 5432




