from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, date
import pandas as pd
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas

# Define default arguments
default_args = {
    'owner': 'Haekal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

snowflake_config = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE_STAGING'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE')
}

def truncate_table(*table_name):
    conn = snowflake.connector.connect(**snowflake_config)
    conn.cursor().execute("USE DATABASE TRANSFERMRKT_STAGING")
    conn.cursor().execute("USE SCHEMA TRANSFERMRKT_SCHEMA")
    conn.cursor().execute("USE ROLE TRANSFER_ADMIN")
    for table in table_name:
        conn.cursor().execute(f"TRUNCATE TABLE IF EXISTS {table}")
    conn.close()

def convert_dates(df):
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].dt.date
    return df

def df_to_snowflake(dataframe,table):
    dataframe = convert_dates(dataframe.copy())
    
    conn = snowflake.connector.connect(**snowflake_config)
    conn.cursor().execute("USE DATABASE TRANSFERMRKT_STAGING")
    conn.cursor().execute("USE SCHEMA TRANSFERMRKT_SCHEMA")
    conn.cursor().execute("USE ROLE TRANSFER_ADMIN")
    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=dataframe,
        table_name=table,
        schema='TRANSFERMRKT_SCHEMA',
        database='TRANSFERMRKT_STAGING',
        quote_identifiers=True
    )
    print(f"Ingestion success: {success}, Chunks: {num_chunks}, Rows: {num_rows}")
    conn.close()

def upload_data(APPEARANCES, CLUBS, CLUB_GAMES, COMPETITIONS, GAMES, PLAYERS, PLAYER_VALUATIONS, TRANSFERS):
    
    appearances_df = pd.read_csv(APPEARANCES, parse_dates=['date'])
    appearances_df.columns = appearances_df.columns.str.upper()
    df_to_snowflake(appearances_df,"APPEARANCES")

    clubs_df = pd.read_csv(CLUBS)
    clubs_df.columns = clubs_df.columns.str.upper()
    clubs_cleaned_df = clubs_df.iloc[:, [0,1,2,3,5,6,7,8,9,10,11,12,14]]
    df_to_snowflake(clubs_cleaned_df,"CLUBS")

    club_games_df = pd.read_csv(CLUB_GAMES)
    club_games_df.columns = club_games_df.columns.str.upper()
    df_to_snowflake(club_games_df,"CLUB_GAMES")

    competitions_df = pd.read_csv(COMPETITIONS)
    competitions_df.columns = competitions_df.columns.str.upper()
    competitions_cleaned_df = competitions_df.iloc[:, [0,1,2,3,4,5,6,7,8,10]]
    df_to_snowflake(competitions_cleaned_df,"COMPETITIONS")

    games_df = pd.read_csv(GAMES)
    games_df.columns = games_df.columns.str.upper()
    games_df['DATE'] = pd.to_datetime(games_df['DATE'])
    no_future_games = games_df.loc[games_df['DATE'] <= pd.Timestamp(date.today())]
    games_cleaned_df = no_future_games.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,19,20,22]]
    df_to_snowflake(games_cleaned_df, "GAMES")

    players_df = pd.read_csv(PLAYERS)
    players_df.columns = players_df.columns.str.upper()
    players_df['DATE_OF_BIRTH'] = pd.to_datetime(players_df['DATE_OF_BIRTH'])
    players_df['CONTRACT_EXPIRATION_DATE'] = pd.to_datetime(players_df['CONTRACT_EXPIRATION_DATE'])
    df_to_snowflake(players_df, "PLAYERS")

    players_valuations_df = pd.read_csv(PLAYER_VALUATIONS)
    players_valuations_df.columns = players_valuations_df.columns.str.upper()
    players_valuations_df['DATE'] = pd.to_datetime(players_valuations_df['DATE'])
    df_to_snowflake(players_valuations_df, "PLAYER_VALUATIONS")

    transfer_df = pd.read_csv(TRANSFERS)
    transfer_df.columns = transfer_df.columns.str.upper()
    transfer_df['TRANSFER_DATE'] = pd.to_datetime(transfer_df['TRANSFER_DATE'])
    df_to_snowflake(transfer_df, "TRANSFERS")

with DAG(
    dag_id='ETL_Transfermrkt',
    default_args=default_args,
    description='end to end trasnfermrkt pipeline using Airflow, DBT and Snowflake',
    schedule='@weekly',
    start_date=datetime(2014,1,1),
    catchup=False
)as dag:
    
    truncate = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table,
        op_args=["APPEARANCES", "CLUBS", "CLUB_GAMES", "COMPETITIONS", "GAMES", "PLAYERS", "PLAYER_VALUATIONS", "TRANSFERS"]
    )

    download = BashOperator(
        task_id="download_data",
        bash_command="scripts/download_data.sh"
    )

    upload= PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_args=["/home/haekal/data_grind/transfermrkt_project/datasets/appearances.csv",
                 "/home/haekal/data_grind/transfermrkt_project/datasets/clubs.csv",
                 "/home/haekal/data_grind/transfermrkt_project/datasets/club_games.csv",
                 "/home/haekal/data_grind/transfermrkt_project/datasets/competitions.csv",
                 "/home/haekal/data_grind/transfermrkt_project/datasets/games.csv",
                 "/home/haekal/data_grind/transfermrkt_project/datasets/players.csv",
                 "/home/haekal/data_grind/transfermrkt_project/datasets/player_valuations.csv",
                 "/home/haekal/data_grind/transfermrkt_project/datasets/transfers.csv"
                 ]
    )

    dbt_run= BashOperator(
        task_id='dbt_run',
        bash_command="scripts/dbt_run.sh"
    )
    
    delete = BashOperator(
        task_id='delete_old_file',
        bash_command="scripts/delete_old_data.sh"
    )

    download >> truncate >> upload >> dbt_run >> delete