import os
import logging
import urllib
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import types, create_engine
from sqlalchemy.engine.url import URL
import psycopg2
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from sqlalchemy import types, create_engine
from sqlalchemy.engine.url import URL
import psycopg2
from sqlalchemy.orm import sessionmaker

path_to_local_home = "/home/airflow/gcs/data/key.json"
scope = ['https://spreadsheets.google.com/feeds']
worksheet_name = 'Purchase_Dataset'
credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_local_home, scope)
spreadsheet_key = 'SpreadsheetKey'
gc = gspread.authorize(credentials)
book = gc.open_by_key(spreadsheet_key)
url = 'SpreadsheetURL'
book = gc.open_by_url(url)
worksheet = book.get_worksheet(index)
table = worksheet.get_all_values()
HistoricalTraining = pd.DataFrame()
HistoricalTraining = HistoricalTraining.append(table)
HistoricalTraining.columns = HistoricalTraining.iloc[0]
HistoricalTraining = HistoricalTraining.reindex(HistoricalTraining.index.drop(0))
server = 'EDM-IYKRA-HANIF' 
database = 'data' 
quoted = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

def truncate_table(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute('''TRUNCATE TABLE dbo.HistoricalTraining''')
    session.commit()
    session.close()


def dataframe_to_sqlserver(engine,dataframe):
    conn = engine.connect()
    dataframe.to_sql('HistoricalTraining', engine,if_exists='replace', index=False)

def report_table(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute('''  SELECT a.EmployeeId,a.Fullname,a.Birthdate,a.PosTitle,
  b.Subject,b.Category,b.Platform,b.Organizer,b.Duration_Week,b.StartDate,b.EndDate INTO dbo.Report_Table_Master
  FROM [dbo].[EmployeeData] AS a
  LEFT JOIN [dbo].[HistoricalTraining] AS b
  ON a.EmployeeId = b.EmployeeId''')
    session.commit()
    session.close()



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="spreadsheet_to_postgresql",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    truncate_sqlserver = PythonOperator(
        task_id="truncate_sqlserver",
        python_callable=truncate_table,
        op_kwargs={
            "engine": engine,
        },
    )

    df_to_sqlserver = PythonOperator(
        task_id="dataframe_to_sqlserver",
        python_callable=dataframe_to_sqlserver,
        op_kwargs={
            "engine": engine,
            "dataframe":HistoricalTraining,
        },
    )

    report_table_sqlserver = PythonOperator(
        task_id="report_table",
        python_callable=report_table,
        op_kwargs={
            "engine": engine
        },
    )

    truncate_sqlserver  >> df_to_sqlserver >> report_table_sqlserver