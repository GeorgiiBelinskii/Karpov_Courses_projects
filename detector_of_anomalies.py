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
    @task
    def check_anomalies_iqr(df, window=5, number_of_iqr=2, critical_level=0.25):
        # calculation 25percentile, 75 percentile and IQR for last 5 values\window

        # df - dataframe
        # column - calculated column from dataframe
        # window - period\window for moving average
        # number_of_iqr  - number of IQR range
        # critical_level - sensitivity of system, percent of difference from previous value
        for column in df.columns[1:]:

            # percentiles of moving average and inter quantile range
            df['m_25p'] = df[column].shift(1).rolling(window).quantile(0.25)
            df['m_75p'] = df[column].shift(1).rolling(window).quantile(0.75)
            df['iqr'] = df.m_75p - df.m_25p

            # calculation up and low boarder of iqr
            df['up'] = df['m_75p'] + number_of_iqr * df['iqr']
            df['up'] = df.up.rolling(window).mean()

            df['low'] = df['m_25p'] - number_of_iqr * df['iqr']
            df['low'] = df.low.rolling(window).mean()

            # last and previous values, which will be comparing
            last = df.iloc[-1][column]
            prev = df[column].iloc[-2]

            if ((last < df.low.iloc[-1]) or (last > df.up.iloc[-1])) and (abs((last / prev) - 1)) > critical_level:
                msg_alert = f''' ALERT!!! IQR-system
    time: {df.period.iloc[-1]}
    measure: {column}
    current value: {last}
    prev value: {prev}
    deviation from previous value {((last / prev) - 1):.2%}
    look up here: link_dashboard
        '''
                bot.sendMessage(chat_id=my_chat_id, text=msg_alert)

                sns.set_theme(style="whitegrid")
                sns.axes_style("darkgrid")
                fig, ax = plt.subplots(figsize=(17, 12))
                dict_colors = {'up': 'red',
                               'low': 'red'}

                for el in [column, 'up', 'low']:
                    ax = sns.lineplot(
                        x='period',
                        y=el,
                        data=df,
                        color=dict_colors.get(el),
                        label=el)
                ax.set_title(f'{column}')

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{column}_anomaly_report.png'
                plt.close()
                bot.sendPhoto(chat_id=my_chat_id, photo=plot_object)

    query_news = '''
                   select toStartOfFifteenMinutes(time) as period,
                        uniqExact(user_id) as news_users,
                        countIf(action='like') as likes,
                        countIf(action='view') as views,
                        likes / views as CTR
                    from simulator_20220720.feed_actions
                    where (time >= today() - 1) and (time < toStartOfFifteenMinutes(now()))
                    group by toStartOfFifteenMinutes(time)
                    order by period
        '''
    query_msg = '''
                   select toStartOfFifteenMinutes(time) as period,
                                uniqExact(user_id) as users_msg,
                            count(user_id) as msg_sent
                    from simulator_20220720.message_actions
                    where (time >= today() - 1) and (time < toStartOfFifteenMinutes(now()))
                    group by toStartOfFifteenMinutes(time)
                    order by period
         '''
    for query in [query_news, query_msg]:
        df = extract_data(query, connection)
        check_anomalies_iqr(df)


detector_of_anomalies()












