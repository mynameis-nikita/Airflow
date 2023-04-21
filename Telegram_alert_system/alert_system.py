import sys
import os
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from datetime import date


import warnings
warnings.filterwarnings("ignore")


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
schedule_interval = '*/15 * * * *'

    
    # Таск hеализует алгоритм поиска аномалий -межквартильный рахмах
def chack_anomaly(df, metric, a=3, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25'] 
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center = True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center = True,  min_periods=1).mean()
        
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else: 
        is_alert = 0
        
    set_to_return = (is_alert, df)
    
    return set_to_return


@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def dag_kozhevatov_8_1():
    
    @task
    def run_alerts(chat = None):
        chat_id = chat or os.environ.get("ALERT_CHAT_ID")
        bot = telegram.Bot(token = os.environ.get("TOKEN"))
        
        query = '''
            SELECT toStartOfFifteenMinutes(time) as ts,
                toDate(time) as date,
                formatDateTime(ts, '%R') as hm,
                uniqExact(user_id) as users_feed,
                countIf(user_id, action = 'view') as views,
                countIf(user_id, action = 'like') as likes
            FROM simulator_20230220.feed_actions
            WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts'''
        
        data = ph.read_clickhouse(query = query, connection = connection)
        
        metrics_list = ['users_feed', 'views', 'likes']
        
        for metric in metrics_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = chack_anomaly(df, metric)
        
            if is_alert == 1: 
                msg = '''Метрика {metric}:\nтекущее значение {current_val:.2f}\n
отклонение от предыдущего значения {last_val_diff:.2%}'''.format(metric = metric, 
                                                                 current_val = df[metric].iloc[-1], 
                                                                 last_val_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))
            
                sns.set(rc={'figure.figsize':(16, 10)})
                plt.tight_layout()
                
                ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label = 'low')
                
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else: 
                        label.set_visible(False)
            
                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)
                
                ax.set_title = (metric)
                ax.set(ylim = (0,None))
                
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)            
                plot_object.seek(0)
                plot_object.name = '0.png'
                plt.close()
                
                bot.sendMessage(chat_id = chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)            
    
        return data



    run_alerts()
dag_kozhevatov_8_1 = dag_kozhevatov_8_1()