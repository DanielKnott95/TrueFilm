import streamlit as st
import pandas as pd

from utils import create_db_connection

db_engine = create_db_connection()


def get_top_films():
    query = """ 
    select title, revenue, budget, rev_ratio as ratio
    from metadata_report
    order by rev_ratio desc
    limit 10 """
    df = pd.read_sql(query, db_engine)
    return df, query

def get_top_genres():
    query = """ 
    select name, avg(rev_ratio) mean_ratio
    from genres
    group by name
    order by mean_ratio desc
    limit 5"""
    df = pd.read_sql(query, db_engine)
    return df, query

def get_top_companies():
    query = """ 
    select name, avg(rev_ratio) mean_ratio
    from production_companies
    group by name
    order by mean_ratio desc
    limit 5"""
    df = pd.read_sql(query, db_engine)
    return df, query

def get_top_film(table, name):
    query = f"""
    select *
    from metadata_report
    where imdb_id in (
        select imdb_id
        from {table}
        where name = '{name}'
        order by rev_ratio desc
        limit 1
    )
    """
    df = pd.read_sql(query, db_engine)
    return df, query


if __name__ == "__main__":
    st.title("TrueFilm Insights Dashboard")
    st.write("This dasboard is a demonstration of how the data warehouse set up in this repository can be used to query the data provided at https://www.kaggle.com/rounakbanik/the-movies-dataset?select=movies_metadata.csv")
    st.write("We can start be querying some of the top performing films based on the ratio of their revenue generated to budget")
    st.subheader("Top 10 Films")
    df1, query1 = get_top_films()
    st.dataframe(df1)
    st.write("The above table is achieved by performing the following query:")
    st.markdown(f"""``` sql
                {query1}
                """)
    st.write("There are two supporting tables for each film: genres and production companies")
    st.write("By using the imdbID as a key we can join these tables to find top performing films by genre and production company")
    st.write("Lets start by querying the top performing genres and production companies by average revenue to budget ratio")

    st.subheader("Top 5 performing genres")
    df2, query2 = get_top_genres()
    st.dataframe(df2)
    st.write("The above table is achieved by performing the following query:")
    st.markdown(f"""``` sql
                {query2}
                """)
    
    st.subheader("Top 5 performing production companies")
    df3, query3 = get_top_companies()
    st.dataframe(df3)
    st.write("The above table is achieved by performing the following query:")
    st.markdown(f"""``` sql
                {query3}
                """)
    
    st.write("")
    st.header("Deeper Analysis")
    st.write("Lets dive a level deeper and explore the top performing films for individual genres and production companies.")
    st.write("By selecting a genre or production company the top film will be displayed, along with its supporting metadata and a link to its wikipedia page.")

    st.subheader("Genres")
    selected_genre = st.selectbox("Select a Genre", pd.read_sql("select distinct name from genres", db_engine)['name'].to_list())
    df4, query4 = get_top_film('genres', selected_genre)
    st.write(f"""
        Top performing film in {selected_genre} genre is {df4.title.iloc[0]}\n
        Access more information at {df4.wiki_url.iloc[0]}
    """)
    st.dataframe(df4)
    st.write("The above table is achieved by performing the following query:")
    st.markdown(f"""``` sql
            {query4}
            """)
    
    st.subheader("Production Comapnies")
    selected_comp = st.selectbox("Select a Production Comapny", pd.read_sql("select distinct name from production_companies", db_engine)['name'].to_list())
    df5, query5 = get_top_film('production_companies', selected_comp)
    st.write(f"""
        Top performing film in {selected_comp} genre is {df5.title.iloc[0]}\n
        Access more information at {df5.wiki_url.iloc[0]}
    """)
    st.dataframe(df5)
    st.write("The above table is achieved by performing the following query:")
    st.markdown(f"""``` sql
            {query5}
            """)
