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
                                    where toDate(time) >= today() -8 and toDate(time) < today()-1
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
        # function for format daily report in message template and sending

        # df - dataframe
        # chat_id  - id of telegram chat, where needs to send the message

        msg = f'Feed report of {df.day[7].date()} \n\n'
        for col in df.columns[1:]:
            # using all columns in dataframe for format main metrics
            msg += f"{col:<7}: {(df[col][7]): {'.2' if col == 'CTR' else ''}}, last day {(df[col][7] / df[col][6] - 1):.2%}, week ago {(df[col][7] / df[col][0] - 1):.2%} \n"

        bot.sendMessage(chat_id=chat_id, text=msg)

    @task
    def plot_and_send_dynamics(df, chat_id):
        sns.set_theme(style="whitegrid")
        sns.axes_style("darkgrid")

        dict_color = {'DAU': 'b',
                      'Likes': 'r',
                      'Views': 'g',
                      'CTR': 'brown',
                      'Events': 'orange',
                      'Posts': 'm',
                      }

        fig, axes = plt.subplots(3, 2, figsize=(20, 20))
        fig.suptitle('7-days dynamics of product')
        for num, col in enumerate(df.columns[1:]):
            sns.lineplot(x='day',
                         y=col,
                         data=df,
                         ax=axes[num % 3][num // 3],
                         color=dict_color[col]).set_title(col)

            axes[num % 3][num // 3].set_ylabel(col)

        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '7-days_report.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    daily_report = extract_data(query)
    report_msg(daily_report, my_chat_id)
    plot_and_send_dynamics(daily_report, my_chat_id)


yesterday_report_with_dynamics()

