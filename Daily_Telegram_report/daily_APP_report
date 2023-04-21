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
def dag_kozhevatov_7_2 ():
    
    @task
    # Таск выгружает метрики приложения за прошлый день / этот день неделю назад / этот день месяц назад
    def extract():
        query = '''
        SELECT * FROM 
            (SELECT toDate(time) as day, 
                    COUNT(distinct user_id) as dau, 
                    CountIf(action = 'view') as views, CountIf(action = 'like') as likes,
                    CountIf(action = 'like') / CountIf(action = 'view') as ctr
            FROM simulator_20230220.feed_actions
            WHERE day = today() - 1 or day = today() - 8  or day = today() - 29  
            GROUP BY toDate(time)) t1
        JOIN 
            (SELECT toDate(time) as day , 
                    count (user_id) as messages
            FROM simulator_20230220.message_actions
            WHERE day = today() - 1 or day = today() - 8  or day = today() - 29  
            GROUP BY toDate(time)) t2
        using day
        order by day asc
        '''
    
        data = ph.read_clickhouse(query = query, connection=connection)
        return data

    @task
    # Таск вычисляет кол-во новых (new Id) пользователей за вчера / тот же день неделю назад / тот же день месяц назад
    def extract_new_users ():
        query = '''
            SELECT 
                toDate(start_day) AS timestamp,
                source,
                count(DISTINCT user_id) AS cnt_dis
            FROM
              (SELECT DISTINCT ON (user_id) user_id,
                                  toDate(time) AS start_day,
                                  source
               FROM simulator_20230220.feed_actions
               order by start_day asc) AS virtual_table
            GROUP BY source, timestamp
            HAVING timestamp = today() - 7 or timestamp = today() - 1 or timestamp = today() - 28
            '''

        new_users = ph.read_clickhouse(query = query, connection=connection)
        return new_users


    @task
    # Таск возвращает пользователей с Рекламного трафика в виде сортированного в хронологическом порядке DF с кол-вом new Id
    def new_users_transform_ads(new_users):
        new_users['days_ago'] = new_users.timestamp.apply(lambda x: new_users.timestamp.max() - x + timedelta(days = 1))
        new_users_ads = new_users[new_users.source == 'ads'].sort_values(by = 'days_ago')
        return new_users_ads   
    
    
    @task
    # Таск возвращает пользователей с Органического трафика в виде сортированного в хронологическом порядке DF с кол-вом new Id
    def new_users_transform_org(new_users):
        new_users['days_ago'] = new_users.timestamp.apply(lambda x: new_users.timestamp.max() - x + timedelta(days = 1))
        new_users_org = new_users[new_users.source == 'organic'].sort_values(by = 'days_ago')
        return new_users_org
    
    @task
    # Таск отправляет в ТГ сводку по кол-ву новых id в сравнении с неделей назад и месяцем назад
    def new_users_info_sender(new_users_org, new_users_ads):
        base = new_users_ads.iloc[0].cnt_dis 
        base1= new_users_org.iloc[0].cnt_dis
        bot.sendMessage(chat_id=chat_id, text = 'Новые пользователи:'+ f'''
"Рекламные"
{base}  |  {round((base -  int(new_users_ads.iloc[1].cnt_dis)) / new_users_ads.iloc[1].cnt_dis * 100, 2)}%   |   { round((base -int(new_users_ads.iloc[2].cnt_dis))/ new_users_ads.iloc[2].cnt_dis * 100,2)}%
    
"Органические"
{base1}  |  {round((base1 - int(new_users_org.iloc[1].cnt_dis))/new_users_org.iloc[1].cnt_dis*100,2) }%   |   { round((base1 -int(new_users_org.iloc[2].cnt_dis))/ new_users_org.iloc[2].cnt_dis * 100,2)}%
''')
    
    
    
    @task  
    def prepare(raw_data): 
        raw_data = raw_data[['dau', 'views', 'likes', 'ctr', 'messages']]
        res_df = pd.DataFrame(index=raw_data.columns, data = {'prev_day': round(raw_data.iloc[-1],2), 
                                "% vs_week_ago": round((raw_data.iloc[-1] - raw_data.iloc[1])/ raw_data.iloc[1] * 100, 2), 
                                "% vs_month_ago":round((raw_data.iloc[-1] - raw_data.iloc[0])/ raw_data.iloc[0] * 100, 2)})
        return res_df

    
    
    @task
    def sender (df):   
        bot.sendMessage(chat_id=chat_id, 
                    text = f'''
Ключевые метрики за
{df.columns.values[0]} в формате:
KBI | vs Week ago | vs Month ago

DAU: 
{int(df.loc['dau'][0]):,}    |   {df.loc['dau'][1]:,}%  |  {df.loc['dau'][2]:,}%

Likes: 
{int(df.loc['likes'][0]):,}  |  {df.loc['likes'][1]:,}%  |  {df.loc['likes'][2]:,}%

Views: 
{int(df.loc['views'][0]):,}  |  {df.loc['views'][1]:,}%  |  {df.loc['views'][2]:,}% 

Messages sent: 
{int(df.loc['messages'][0]):,}   |   {df.loc['messages'][1]:,}%   |   {df.loc['messages'][2]:,}% 
 
CTR: 
{df.loc['ctr'][0]:,}   |   {df.loc['ctr'][1]:,}%   |  {df.loc['ctr'][2]:,}% 

''')
    
    
    data = extract()
    df = prepare(data)
    sender(df)
    new_users = extract_new_users()
    new_users_org = new_users_transform_org(new_users)
    new_users_ads = new_users_transform_ads(new_users)
    new_users_info_sender(new_users_org, new_users_ads)

dag_kozhevatov_7_2 = dag_kozhevatov_7_2()