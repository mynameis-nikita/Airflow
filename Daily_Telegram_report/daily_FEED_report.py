import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task

import warnings
warnings.filterwarnings("ignore")



# ТОКЕН для доступа к боту
my_token = os.environ.get("TOKEN") 
bot = telegram.Bot(token=my_token) # получаем доступ

# id Чата
chat_id = os.environ.get("CHAT_ID")


# Параметры для подключения к базе CH с данными для выгрузки
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20230220',
    'user':os.environ.get("DB_LOGIN"),
    'password':os.environ.get("DB_PASS")
}


# Дефолтные параметры для dag, которые прокидываются в таски
default_args = {
    'owner': 'n.kozhevjatov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 23),
}


# Интервал запуска DAG (Каждый день в 11:00)
schedule_interval = '0 11 * * *'


@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def dag_kozhevatov_7_1 ():
    
    @task
    #  Таск выгружает необходимые метрики за последние 7 дней
    def extract_feed():
        query_week = '''
        SELECT
            toDate(time) as date,
            count(DISTINCT user_id) as dau, 
            CountIf(action = 'like') as likes, 
            CountIf(action = 'view') as views,  
            likes / views as ctr
        FROM simulator_20230220.feed_actions
        WHERE toDate(time) <= today() - 1 and toDate(time) > today() - 8
        GROUP BY date
        '''
        
        feed_data = ph.read_clickhouse(query = query_week, connection=connection)
        return feed_data
    
    
    @task
    #  Таск возвращает значения метрик за вчерашний день
    def transform(feed_data):
        date = feed_data.iloc[-1]['date']
        dau = feed_data.iloc[-1]['dau']
        likes = feed_data.iloc[-1]['likes']
        views = feed_data.iloc[-1]['views']
        ctr = feed_data.iloc[-1]['ctr']
          
        bot.sendMessage(chat_id=chat_id, 
                        text = 
    f'''{date.day} {date.month_name()} {date.year}: 
    DAU = {dau:,}
    Likes = {likes:,}
    Views = {views:,}
    CTR = {round(ctr,3)}
    ''')
        
    
    
    @task
    #  Таск отправляет графики по основным метрикам Просмотры/ Лайки/ CTR/ DAU
    def plot_builder (feed_data):
        
        plt.figure(figsize=(15,15))
        plt.subplots_adjust(hspace=0.3, wspace=0.3)
           
        plt.subplot(2,2,1)
        plt.title('VIEWS')
        sns.lineplot(data = feed_data, x ='date', y='views', markers=True)
        plt.grid()
        plt.xticks(rotation = 25)
        
        plt.subplot(2,2,2)
        sns.lineplot(data = feed_data, x ='date', y='likes')
        plt.title('LIKES')
        plt.grid()
        plt.xticks(rotation = 25)
        
        plt.subplot(2,2,3)
        plt.title('CTR')
        sns.lineplot(feed_data.date, feed_data.ctr)
        plt.grid()
        plt.xticks(rotation = 25)
        
        plt.subplot(2,2,4)
        sns.lineplot(feed_data.date, feed_data.dau)
        plt.title('DAU')
        plt.grid()
        plt.xticks(rotation = 25);
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    

    
    feed_data = extract_feed()    
    transform(feed_data)
    plot_builder(feed_data)

dag_kozhevatov_7_1 = dag_kozhevatov_7_1()