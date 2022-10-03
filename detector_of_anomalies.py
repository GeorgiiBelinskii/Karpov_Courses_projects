import configparser
from datetime import datetime, timedelta
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram
from airflow.decorators import dag, task

# import and parse configs data for connection to database
config = configparser.ConfigParser()
config.read('configs.ini')

connection = {
    'host': config['db']['host'],
    'database': config['db']['database'],
    'user': config['db']['user'],
    'password': config['db']['password']
}
# DAGs info in Airflow

default_args = {
    'owner': config['db']['owner'],
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=35),
    'start_date': datetime(2022, 8, 8),
}
# Interval for DAG
schedule = '1 11 * * *'

# telegram bot init
my_token = config['telegram_bot']['token']
group_chat_id = config['telegram_bot']['group_chat_id']
my_chat_id = config['telegram_bot']['my_chat_id']

bot = telegram.Bot(token=my_token)


@dag(default_args=default_args, schedule=schedule, catchup=False)
def detector_of_anomalies():
    @task
    def extract_data(query, connection):
        # function for extract data from database

        # query - SQL-query for Clickhouse db
        df = ph.read_clickhouse(query, connection=connection)

        change_type_columns = df.select_dtypes(include='uint').columns
        for col in change_type_columns:
            df[col] = df[col].astype('int')
        return df

    @task
    def check_anomalies_iqr(df, window=5, number_of_iqr=2, critical_level=0.1):
        pass



