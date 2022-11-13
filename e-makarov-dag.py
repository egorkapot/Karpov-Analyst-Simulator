from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


#default args это необходимые параметры для запуска тасков. Указываются в обертке дага
default_args = {
    'owner': 'e.makarov', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries':55, # Кол-во попыток выполнить DAG
    'retry_delay':timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 10, 16) # Дата начала выполнения DAG
}

# cron-выражение, также можно использовать '@daily', '@weekly', а также timedelta
#Интервал запуска DAG
schedule_interval = '0 12 * * *'
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
    ph.to_clickhouse(df, 'etl_emakarov', connection=connection, index=False)
    
#По тз нужно было создать таблицу в кликхаусе. Ниже код который создает таблицу

query_table = """ 

CREATE TABLE IF NOT EXISTS test.etl_emakarov
(event_date Date,
dimension String, 
dimension_value String,
views UInt64,
likes UInt64,
messages_received UInt64,
messages_sent UInt64,
users_received UInt64,
users_sent UInt64)
ENGINE = MergeTree()
ORDER BY event_date

"""
    
#В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. 

#Каждая выгрузка должна быть в отдельном таске.    
    
query_feed = """
SELECT user_id as user, toDate(time) as event_date, age, gender, os, countIf(action='view') AS views,
    countIf(action='like') AS likes
FROM simulator_20220920.feed_actions
WHERE toDate(time) = yesterday()
GROUP BY user_id, toDate(time), age, gender, os
"""

#В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. 
query_messages = """

WITH messages_received as (
SELECT reciever_id as user, COUNT(reciever_id) as messages_received, uniq(user_id) as users_sent
FROM simulator_20220920.message_actions
WHERE toDate(time) = yesterday()
GROUP BY reciever_id
)

SELECT *
FROM
(SELECT user_id as user, toDate(time) as event_date, age, gender, os, COUNT(user_id) as messages_sent, uniq(reciever_id) as users_received
FROM simulator_20220920.message_actions
WHERE toDate(time) = yesterday()
GROUP BY user_id, toDate(time) as event_date, age, gender, os) ms
JOIN messages_received mr USING (user)

"""


    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_emakarov_2():
    #Функция extract
    @task
    def extract(query):
        df = ch_get_df(query=query)
        return df
    
    #Две таблицы в одну
    @task 
    def transform_merge (df_feed, df_messages):
        df_cube = df_feed.merge(df_messages, how='outer', on=['user', 'event_date', 'os', 'gender', 'age']).replace(np.nan, 0)
        return df_cube  
    
    #Функция трансформации. Берет объект df_cube. Под капотом отдельные функции, с разными трансформациями       
    @task
    def transform_os(df_cube):
        df_cube_os = df_cube[['os', 'event_date', 'views', 'likes', 'messages_received', 'messages_sent', \
            'users_received', 'users_sent']].groupby(['event_date','os']).sum().reset_index()
        df_cube_os.rename(columns={'os': 'dimension_value'}, inplace=True)
        df_cube_os.insert(1, 'dimension', 'os')  
        return df_cube_os
            
    #Функция которая группирует по gender
    @task
    def transform_gender(df_cube):
        df_cube_gender = df_cube[['gender', 'event_date', 'views', 'likes', 'messages_received', 'messages_sent', \
             'users_received', 'users_sent']].groupby(['event_date','gender']).sum().reset_index()
        df_cube_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)
        df_cube_gender.insert(1, 'dimension', 'gender')          
        return df_cube_gender

    #Функция которая группирует по age
    @task
    def transform_age(df_cube):
        df_cube_age = df_cube[['age', 'event_date', 'views', 'likes', 'messages_received', 'messages_sent', \
            'users_received', 'users_sent']].groupby(['event_date','age']).sum().reset_index()
        df_cube_age.rename(columns={'age': 'dimension_value'}, inplace=True)
        df_cube_age.insert(1, 'dimension', 'age') 
        return df_cube_age
    
    #Создаю финальный датафрейм
    @task
    def load_final (df_cube_os, df_cube_gender, df_cube_age):
        types = {#'event_date': date,
        'dimension': object, 
        'dimension_value': object,
        'views': np.int64,
        'likes': np.int64,
        'messages_received': np.int64,
        'messages_sent': np.int64,
        'users_received': np.int64,
        'users_sent': np.int64}
        df_final = pd.concat([df_cube_os, df_cube_gender, df_cube_age]).astype(types)  
        ch_load_df(df_final, query_table)
           
    #Получаю два датафрейма
    df_feed = extract(query_feed)
    df_messages = extract(query_messages) 
    
    #Делаю объект df_cube
    df_cube = transform_merge(df_feed, df_messages)

    #Делаю DataFrames
    df_cube_os = transform_os(df_cube)    
    df_cube_gender = transform_gender(df_cube)
    df_cube_age = transform_age(df_cube)
    #Загружаю
    load_final (df_cube_os, df_cube_gender, df_cube_age)
    

dag_emakarov_2 = dag_emakarov_2()