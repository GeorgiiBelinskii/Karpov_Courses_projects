import configparser
from datetime import datetime, timedelta
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram
from airflow.decorators import dag, task

# parsing configs data for connection to database
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
schedule_interval = '1 11 * * *'

# telegram bot init
my_token = config['telegram_bot']['token']
group_chat_id = config['telegram_bot']['group_chat_id']
my_chat_id = config['telegram_bot']['my_chat_id']

bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule=schedule_interval, catchup=False)
def yesterday_report_with_dynamics():


    @task
    def extract_data(query):
        pass

    @task
    def report_msg(df, chat_id):
        pass
    @task
    def plot_and_send_dynamics(df, chat_id):

