import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
import math 
import matplotlib.gridspec as gridspec


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

my_token = '5368989134:AAF2Y7F-S0X95XYZnGFO7u123-AqZZmxFTc' # тут нужно заменить на токен вашего бота

bot = telegram.Bot(token=my_token) # получаем доступ

#Функции чтобы получить информацию об отправителях
#updates = bot.getUpdates()
#print(updates[-1])

chat_id = '-769752736'
maxim_chat_id = '591275478'
my_chat_id = '798247808'

#default args это необходимые параметры для запуска тасков. Указываются в обертке дага
default_args = {
    'owner': 'e.makarov', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries':55, # Кол-во попыток выполнить DAG
    'retry_delay':timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 10, 19) # Дата начала выполнения DAG
}

# cron-выражение, также можно использовать '@daily', '@weekly', а также timedelta
#Интервал запуска DAG
schedule_interval = '0 11 * * *'
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

query_feed_yesterday = """ 
SELECT toDate(time) AS day, COUNT(DISTINCT user_id) as DAU, 
countIf(action='like') as likes,
countIf(action='view') as views,
likes/views *100 as ctr
FROM simulator_20220920.feed_actions
WHERE toDate(time) = yesterday()
GROUP BY toDate(time)
"""
query_feed_week_ago = """ 
SELECT toDate(time) AS day, COUNT(DISTINCT user_id) as DAU, 
countIf(action='like') as likes,
countIf(action='view') as views,
likes/views *100 as ctr
FROM simulator_20220920.feed_actions
WHERE toDate(time) >= today() - 7
GROUP BY toDate(time)
"""

query_messages_yesterday = """

WITH messages_received as (
SELECT reciever_id as user, COUNT(reciever_id) as messages_received, uniq(user_id) as users_sent
FROM simulator_20220920.message_actions
WHERE toDate(time) = yesterday()
GROUP BY reciever_id
)

SELECT event_date, os, gender, age, SUM(messages_sent) as messages_sent, SUM(messages_received) as messages_received, SUM(users_sent) as users_sent, SUM(users_received) as users_received
FROM
(SELECT user_id as user, toDate(time) as event_date, age, gender, os, COUNT(user_id) as messages_sent, uniq(reciever_id) as users_received
FROM simulator_20220920.message_actions
WHERE toDate(time) = yesterday()
GROUP BY user_id, toDate(time) as event_date, age, gender, os) ms
JOIN messages_received mr USING (user)
GROUP BY event_date, os, gender, age

"""

query_retention = """
SELECT this_week, previous_week, -uniq(user_id) as num_users, status 

FROM
(SELECT user_id, 
groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
addWeeks(arrayJoin(weeks_visited), +1) this_week, 
if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status, 
addWeeks(this_week, -1) as previous_week
FROM simulator_20220920.feed_actions
WHERE (user_id IN (SELECT DISTINCT user_id FROM simulator_20220920.message_actions)) AND (toDate(time) >= today() - 30)
group by user_id)

where status = 'gone'

group by this_week, previous_week, status

HAVING this_week != addWeeks(toMonday(today()), +1)

union all


SELECT this_week, previous_week, toInt64(uniq(user_id)) as num_users, status FROM

(SELECT user_id, 
groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
arrayJoin(weeks_visited) this_week, 
if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status, 
addWeeks(this_week, -1) as previous_week
FROM simulator_20220920.feed_actions
WHERE (user_id IN (SELECT DISTINCT user_id FROM simulator_20220920.message_actions)) AND (toDate(time) >= today() - 30)
group by user_id)

group by this_week, previous_week, status

"""

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_emakarov_report():
    
    @task
    def extract_df(query):
        df = ch_get_df(query)
        return df

    @task
    def send_message(df_feed_yesterday, df_messages_yesterday):
        day = pd.to_datetime("today").date() - pd.Timedelta(1, unit='D')
        #Создаю группу, чтобы посчитать метрики за день
        messages_group = df_messages_yesterday.groupby('event_date').agg({'messages_sent': 'sum', 'messages_received': 'sum', \
     'users_sent': 'sum', 'users_received': 'sum'}).reset_index()

        text_report = f"""Привет, хозяин, я вчера что-то забегался и забыл отправить репорт. Прошу прощения, вот репорт за {day}
        <b>Новостная лента</b>
        ------------------------------------------
        Количество активных читаталей новостной ленты - <u>{df_feed_yesterday.DAU[0]}</u>
        Количество просмотров новостей - <u>{df_feed_yesterday.views[0]}</u>
        Количество лайков - <u>{df_feed_yesterday.likes[0]}</u>
        CTR равен <u>{df_feed_yesterday.ctr[0]:.2f}%</u>
        ------------------------------------------
        <b>Сообщения</b>
        Количество отправленных сообщений - <u>{messages_group.messages_sent[0]}</u>
        Количество полученных сообщений - <u>{messages_group.messages_received[0]}</u>
        Пользоветели которые отправили сообщения  - <u>{messages_group.users_sent[0]}</u>
        Пользователи которые получили сообщения - <u>{messages_group.users_received[0]}</u>
        """
        bot.sendMessage(chat_id = chat_id, text=text_report, parse_mode = 'html')

    @task
    def send_photo_feed(df):
        fig = plt.figure(figsize = [20, 15]) 
        #Показываем что нам нужны 2 графика по оси Х и 2 графика по оси У
        gs = gridspec.GridSpec(2, 2)
        #Назначаем аксисы графиков
        ax = plt.subplot(gs[0, 0])
        bx = plt.subplot(gs[0, 1])
        cx = plt.subplot(gs[1, 0])
        dx = plt.subplot(gs[1, 1])
        ax.plot(df['day'], df['views'], marker = 'o', linewidth=3, color = 'black')
        ax.set_title('Views', fontsize=20)
        #Размеры тиков
        ax.tick_params(axis='x', which='major', labelsize=12, labelrotation = 30)
        ax.tick_params(axis='y', which='major', labelsize=17)
        bx.plot(df['day'], df['likes'], marker = 'o', linewidth=3, color = 'black')
        bx.set_title('Likes', fontsize=20)
        bx.tick_params(axis='x', which='major', labelsize=12, labelrotation = 30)
        bx.tick_params(axis='y', which='major', labelsize=17)
        cx.plot(df['day'], df['DAU'], marker = 'o', linewidth=3, color = 'black')
        cx.set_title('DAU', fontsize=20)
        cx.tick_params(axis='x', which='major', labelsize=12, labelrotation = 30)
        cx.tick_params(axis='y', which='major', labelsize=17)
        dx.plot(df['day'], df['ctr'], marker = 'o', linewidth=3, color = 'black')
        dx.set_title('CTR', fontsize=20)
        dx.tick_params(axis='x', which='major', labelsize=12, labelrotation = 30)
        dx.tick_params(axis='y', which='major', labelsize=17)
        fig.suptitle(f'Report for {datetime.date(datetime.today() - timedelta(days=8))} - {datetime.date(datetime.today() - timedelta(days=1))}', fontsize=40)
        #Библиотека ИО сохранаят график в буфер и отправляет график как файловый объект. Это делается чтобы не засорять память
        plot_object = io.BytesIO()
        #Сохраняем график в файловый объект
        fig.savefig(plot_object)
        #Двигаем курсор чтобы не было пустого файла
        plot_object.seek(0)
        #Даем имя
        plot_object.name = f'Report_{datetime.date(datetime.today() - timedelta(days=1))}.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    @task
    def send_retention(df_retention):

        df_retention_plot = df_retention.groupby(['this_week', \
            'status']).agg({'num_users': 'max'}).reset_index()
        df_retention_plot['this_week'] = df_retention_plot['this_week'].dt.date
        ax = sns.barplot(data = df_retention_plot, x=df_retention_plot.this_week, y='num_users', hue='status')
        sns.set(rc={'figure.figsize':(11.7,8.27)}, )
        fig = ax.get_figure()
        ax.set_xlabel("Week", fontsize = 20)
        ax.set_ylabel("Number of users", fontsize = 20)
        ax.set_title(f'Retention for {datetime.date(datetime.today())}', fontsize =30)
        plot_object_1 = io.BytesIO()
        #Сохраняем график в файловый объект
        fig.savefig(plot_object_1)
        #Двигаем курсор чтобы не было пустого файла
        plot_object_1.seek(0)
        #Даем имя
        plot_object_1.name = f'Retention for {datetime.date(datetime.today())}.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_1)




    
    
    #Получаем df по новостям за вчера 
    df_feed_yesterday = extract_df(query_feed_yesterday)
    df_messages_yesterday = extract_df(query_messages_yesterday)
    df_retention = extract_df(query_retention)
    df_feed_week_ago = extract_df(query_feed_week_ago)
    send_message(df_feed_yesterday, df_messages_yesterday)
    send_photo_feed(df_feed_week_ago)
    send_retention(df_retention)

    

    
dag_emakarov_report = dag_emakarov_report()


