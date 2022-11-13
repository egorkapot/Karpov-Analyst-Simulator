import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


my_token = '5368989134:AAF2Y7F-S0X95XYZnGFO7u123-AqZZmxFTc' # тут нужно заменить на токен вашего бота

bot = telegram.Bot(token=my_token) # получаем доступ

#Функции чтобы получить информацию об отправителях
#updates = bot.getUpdates()
#print(updates[-1])

chat_id = '-769752736'
my_chat_id = '798247808'

#default args это необходимые параметры для запуска тасков. Указываются в обертке дага
default_args = {
    'owner': 'e.makarov', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries':10, # Кол-во попыток выполнить DAG
    'retry_delay':timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 11, 1) # Дата начала выполнения DAG
}

# cron-выражение, также можно использовать '@daily', '@weekly', а также timedelta
#Интервал запуска DAG
schedule_interval = '*/15 * * * *'
#connection
connection_simulator = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220920',
                      'user':'student',
                    'password':'dpo_python_2020'
                     }

connection_test = {'host':'https://clickhouse.lab.karpov.courses',
                   'database': 'test', 
                  'user':'student-rw', 
                  'password':'656e2b0c9c'

}

def ch_get_df(query='SELECT 1', connection=connection_simulator):
    df = ph.read_clickhouse(query, connection=connection)
    return df

def ch_load_df(df, query='SELECT 1', connection=connection_test):
    ph.execute(query=query, connection=connection)
    ph.to_clickhouse(df, 'etl_emakarov_report', connection=connection, index=False)



query = '''
SELECT * FROM
    (SELECT 
        toStartOfFifteenMinutes(time) as ts, 
        formatDateTime(ts, '%R') as hm,
        countIf(action = 'view') as views, 
        countIf(action = 'like') as likes,
        count(distinct user_id) as users,
        likes / views as ctr
    FROM simulator_20220920.feed_actions 
    WHERE toStartOfFifteenMinutes(time) >= today() - 7 and toStartOfFifteenMinutes(time) < toStartOfFifteenMinutes(now())
    GROUP BY toStartOfFifteenMinutes(time), hm
    ORDER BY ts) as t1
join
    (SELECT 
        toStartOfFifteenMinutes(time) as ts, 
        formatDateTime(ts, '%R') as hm,
        count(user_id) as messages,
        count(distinct user_id) as DAU_messager
    FROM simulator_20220920.message_actions 
    WHERE toStartOfFifteenMinutes(time) >= yesterday() - 7 and toStartOfFifteenMinutes(time) < toStartOfFifteenMinutes(now())
    GROUP BY toStartOfFifteenMinutes(time), hm
    ORDER BY ts) as t2
    using ts, hm
    '''

def check_anomaly_sigma(df, metric):
    current_ts = df['ts'].max() #Достаем последнюю 15-минутку
    #day_ago_ts = current_ts - pd.DateOffset(days=1) #Достаем 15 минутку 1 день назад
    df_current = df[df['ts'] == current_ts]
    #df_day_ago = df[df['ts'] == day_ago_ts]
    df_hour_ago = df[(df['ts'] >= current_ts - pd.Timedelta(hours=1)) & (df['ts'] < current_ts)] #Достаем значения за час назад
    std = df_hour_ago[metric].std() #Стандартное отклонение для метрики 
    mean = df_hour_ago[metric].mean()
    current_value = df_current[metric].iloc[0]
    zscore = (current_value-mean)/std #Является ли текущее значение аномалией для данных час назад
    df_total = pd.concat([df_hour_ago, df_current]) #Делаю общий DF чтобы строить график
    
    if (current_value < mean - 3 * std) | (current_value > mean + 3 * std):
        is_alert_anomaly_sigma = 1
    else:
        is_alert_anomaly_sigma = 0

    return is_alert_anomaly_sigma, current_value, zscore, df_total


def check_interquartile_range(df, metric):

    current_ts = df['ts'].max() #Достаем последнюю 15-минутку
    day_week_ago_ts = current_ts - pd.DateOffset(weeks=1) #Достаем 15 минутку 1 день назад
    df_current = df[df['ts'] == current_ts]
    df_week_ago = df[(df['ts'] >= day_week_ago_ts - pd.Timedelta(minutes = 30)) & (df['ts'] < day_week_ago_ts + pd.Timedelta(minutes = 30))] #Беру 30 минут до и 30 минут после за неделю назад
    q_25 = df_week_ago[metric].quantile(q=0.25)
    q_75 = df_week_ago[metric].quantile(q=0.75)
    iqr = q_75-q_25
    current_value = df_current[metric].iloc[0]
    value_week_ago = df_week_ago[metric].mean()

    if (current_value < q_25 - 1.5 * iqr) | (current_value > q_75 + 1.5 * iqr):
        is_alert_interquartile_range = 1
    else:
        is_alert_interquartile_range = 0

    return is_alert_interquartile_range, value_week_ago

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_emakarov_alert():

    @task
    def bot_send(df, metrics):
        for metric in metrics:
            is_alert_anomaly_sigma, current_value, zscore, df_total = check_anomaly_sigma(df, metric)
            is_alert_interquartile_range, value_week_ago = check_interquartile_range(df, metric)

            if (is_alert_anomaly_sigma) and (is_alert_interquartile_range):
                msg = f"""
                !ALERT! 
                @egorkapot
                Время: {df_total['hm'].iloc[4]}
                Метрика: {metric}
                Текущее значение {current_value:.2f}
                Индекс стандартного отклонения метрики от значений за прошлый час {zscore:.2f}
                Среднее значение метрики в диапазоне 1 час за неделю назад {value_week_ago:.2f}
                """

                fig = plt.figure(figsize = [20, 15])
                plt.plot(df_total['hm'], df_total[metric], marker = 'o', linewidth=3, color = 'black')
                plt.title(f'Report for {metric}', fontsize=20)
                plt.tick_params(axis='x', which='major', labelsize=20, labelrotation = 30)
                plt.tick_params(axis='y', which='major', labelsize=20)
                plot_object = io.BytesIO()
                #Сохраняем график в файловый объект
                fig.savefig(plot_object)
                #Двигаем курсор чтобы не было пустого файла
                plot_object.seek(0)
                #Даем имя
                plot_object.name = f'Report for {metric} as for {datetime.date(datetime.today() - timedelta(days=1))}.png'
                plt.close()
                
                bot.sendMessage(chat_id=my_chat_id, text=msg)
                bot.sendPhoto(chat_id=my_chat_id, photo=plot_object)

    metrics = ['likes', 'views', 'users', 'ctr', 'messages']
    df = ch_get_df(query=query)
    bot_send(df, metrics=metrics)

dag_emakarov_alert = dag_emakarov_alert()