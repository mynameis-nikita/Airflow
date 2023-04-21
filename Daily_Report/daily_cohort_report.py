import pandas as pd
import pandahouse as ph
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import ttest_ind
from scipy import stats
from datetime import datetime, timedelta
from airflow.decorators import dag, task

import warnings
warnings.filterwarnings("ignore")


# Параметры для подключения к базе CH с данными для выгрузки
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20230220',
    'user':os.environ.get("DB_LOGIN"),
    'password':os.environ.get("DB_PASS")
}


# Параметры для подключения к базе CH куда выгружаются готовые отчеты
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':os.environ.get("test_DB_LOGIN"),
    'password':os.environ.get("test_DB_PASS")

}


# Дефолтные параметры для dag, которые прокидываются в таски
default_args = {
    'owner': 'n.kozhevjatov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 20),
}


# Интервал запуска DAG
schedule_interval = '0 23 * * *'  # каждый день в 23:00


@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def dag_kozhevatov ():
        
    @task
    # Таск выгружает в DataFrame кол-во лайков и просмотров для каждого юзера за вчерашний день
    def extract_feed ():
        query_feed = '''SELECT 
                            user_id, os, gender, age, toDate(time) as event_date,
                            CountIf(action = 'like') as likes, 
                            CountIf(action = 'view') as views
                        FROM {db}.feed_actions
                        WHERE toDate(time) = today() - 1
                        GROUP BY user_id, os, gender, age, event_date'''
        df_feed = ph.read_clickhouse(query = query_feed, connection=connection)
        return df_feed
    
        
    @task
    # Таск выгружает в DataFrame кол-во отпр/получ. сообщений и кол-во человек кому каждый юзер отправил сообщ.
    # и кол-во человек от кого этот юзер получил сообщения за вчерашний день.
    def extract_mess ():
        query_mess = '''
            WITH 
            users AS (
                SELECT distinct user_id as user_id, os, gender, age
                FROM simulator_20230220.message_actions),
            
            recieve AS (
                SELECT  reciever_id, count(reciever_id) as messages_received, 
                        count(distinct user_id) as users_received
                FROM simulator_20230220.message_actions
                WHERE toDate(time) = today() - 1
                GROUP BY reciever_id),
            
            sent AS (
                SELECT user_id, count(user_id) messages_sent, 
                       count(distinct reciever_id) users_sent, 
                       toDate(time) event_date
                FROM simulator_20230220.message_actions
                WHERE toDate(time) = today() - 1
                GROUP BY user_id, event_date)
            
            SELECT * FROM 
                 (SELECT * FROM users
                 FULL outer join sent
                 on sent.user_id = users.user_id) A
            FULL outer join recieve
            on A.user_id = recieve.reciever_id
            WHERE event_date = today() - 1 '''
        
        df_mess = ph.read_clickhouse(query = query_mess, connection=connection)
        return df_mess
    
        
    @task
    # Таск объединяет данные тасков extract_feed и extract_mes
    def join_extracts (df_feed, df_mess):        
        merged_data = df_feed.merge(df_mess, how='outer', on= ['user_id', 'os', 'gender', 'age', 'event_date'])
        return merged_data
    
    
    @task
    # Таск суммирует информацию в разрезе возраста   
    def transform_age (merged_data):
        age_report = merged_data.drop(columns=['gender', 'os','user_id','reciever_id'], axis = 1) \
            .groupby(['age', 'event_date'], as_index=False) \
            .sum() \
            .rename(columns = {'age':'dimension_value'})
        age_report.insert(0, 'dimension', 'age')
        return age_report

           
    @task
    # Таск суммирует информацию в разрезе пола   
    def transform_gender (merged_data):
        gender_report = merged_data.drop(columns=['age', 'os','user_id','reciever_id'], axis = 1) \
            .groupby(['gender', 'event_date'], as_index=False) \
            .sum() \
            .rename(columns = {'gender':'dimension_value'})
        gender_report.insert(0, 'dimension', 'gender')
        return gender_report

    @task
    # Таск суммирует информацию в разрезе ОС   
    def transform_os (merged_data):
        os_report = merged_data.drop(columns=['age', 'gender','user_id','reciever_id'], axis = 1) \
            .groupby(['os', 'event_date'], as_index=False) \
            .sum() \
            .rename(columns = {'os':'dimension_value'})
        os_report.insert(0, 'dimension', 'os')
        return os_report
    
    @task
    # Таск объединяет информацию в различных разрезах в один Датафрейм
    def join_transforms (gender_report, os_report, age_report):
        concat_reports = pd.concat([gender_report, os_report, age_report], axis=0)
        cols = ['views', 'likes','messages_received', 'messages_sent','users_received', 'users_sent']
        concat_reports[cols] = concat_reports[cols].astype(int)
        concat_reports= concat_reports[['event_date','dimension','dimension_value','views','likes','messages_received','messages_sent','users_received','users_sent']]

        return concat_reports 
        
        
    @task
    # Таск вносит объединенные данные в таблицу kozhevatov_dag схемы данных test
    def load (concat_reports):
        query = '''  CREATE TABLE if not exists test.kozhevatov_dag
                                             (event_date date,
                                              dimension String,
                                              dimension_value String,
                                              views Int32,
                                              likes Int32,
                                              messages_received Int32,
                                              messages_sent Int32,
                                              users_received Int32,
                                              users_sent Int32) 
                                              ENGINE = MergeTree()
                                              ORDER BY event_date''' 
        
        ph.execute(query, connection=connection_test)
                   
                   
        # Выгрузка датафрейма в базу данных test Clickhouse
        ph.to_clickhouse(concat_reports, 'kozhevatov_dag', connection=connection_test, index=False)


    
    df_feed = extract_feed()
    df_mes = extract_mess()
    merged_data = join_extracts(df_feed, df_mes)
    gender_report = transform_gender(merged_data)
    os_report = transform_os(merged_data) 
    age_report = transform_age(merged_data)
    concat_reports = join_transforms(gender_report, os_report, age_report)
    load(concat_reports)
    
dag_kozhevatov_test = dag_kozhevatov()
