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
    query = '''
                                  select toDate(time) as day,
                                            count(distinct user_id) as DAU,
                                            countIf(action = 'view') as Views,
                                            countIf(action = 'like') as Likes,
                                            Likes / Views as CTR,
                                            Likes + Views as Events,
                                            count(distinct post_id) as Posts
                                    from simulator_20220720.feed_actions
                                    where toDate(time) >= today() -48 and toDate(time) < today()-40
                                    group by toDate(time)
                                    order by toDate(time)
                                '''

    @task
    def extract_data(query):
        # function for extract data from database

        # query - SQL-query for Clickhouse db
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task
    def report_msg(df, chat_id):
        pass
    @task
    def plot_and_send_dynamics(df, chat_id):

