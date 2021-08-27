from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pycoingecko import CoinGeckoAPI
import requests
import pandas as pd
import time
from pandas.io import gbq
from airflow.utils.dates import days_ago
from google.oauth2 import service_account


cg = CoinGeckoAPI()

def load_inputs(ti):   
    ids_list = ['bitcoin','ethereum'] # add new cryptos to include them in the database
    vs_currencies_list = ['usd','eur','brl'] # add new currencies to include them in the database
    ti.xcom_push(key='ids_list', value=ids_list)
    ti.xcom_push(key='vs_currencies_list', value=vs_currencies_list)
    return 

def create_price_url(ti):
    ids_list = ti.xcom_pull(key='ids_list', task_ids='load_inputs')
    vs_currencies_list = ti.xcom_pull(key='vs_currencies_list', task_ids='load_inputs')

    if type(ids_list)!= list or type(vs_currencies_list)!=list:
        raise Exception("create_price_url only accepts lists as arguments!")

    urls_dict = {}
    unformatted_url = 'https://api.coingecko.com/api/v3/coins/{}/market_chart/range?vs_currency={}&from={}&to={}'
    starting_unix = '1609408800'
    ending_unix = datetime.date(datetime.fromtimestamp(time.time())).strftime("%s")
    for id_ in ids_list:
        for currency in vs_currencies_list:
            price_url = unformatted_url.format(id_,currency,starting_unix,ending_unix)
            urls_dict[id_+'_'+currency] = price_url   
        
    ti.xcom_push(key='urls_dict', value=urls_dict)
    return 

def create_unix_dates(ti): # creates the date list, which will be the dates column in the db

    urls_dict = ti.xcom_pull(key='urls_dict', task_ids='create_prices_urls')
    ids_list = ti.xcom_pull(key='ids_list', task_ids='load_inputs')

    unix_dates = [request[0] for request in requests.get(urls_dict['bitcoin_brl']).json()['prices']]*len(ids_list)

    ti.xcom_push(key='unix_dates', value=unix_dates)
    return 

def create_prices_list(ti):
    urls_dict = ti.xcom_pull(key='urls_dict', task_ids='create_prices_urls')
    vs_currencies_list = ti.xcom_pull(key='vs_currencies_list', task_ids='load_inputs')

    prices_list_dict = {}
    for url in urls_dict:
        prices_list_dict[url] = [request[1] for request in requests.get(urls_dict[url]).json()['prices']] # creates a price list for each crypto/currency pair

    final_columns_dict = {} # stacks all price lists for a given currency. The resulting lists will be the dataframe price  columns. 
    for currency in vs_currencies_list: 
        for price_list in prices_list_dict.keys():
            if currency in price_list:
                if currency+'_list' in final_columns_dict:
                    final_columns_dict[currency+'_list'] = final_columns_dict[currency+'_list'] + prices_list_dict[price_list]
                else:
                    final_columns_dict[currency+'_list'] = prices_list_dict[price_list]

    ti.xcom_push(key='final_columns_dict', value=final_columns_dict)

    return 

def create_infos_list(ti): 

    infos_json = requests.get('https://api.coingecko.com/api/v3/coins/list?include_platform=false').json()
    ids_list = ti.xcom_pull(key='ids_list', task_ids='load_inputs')
    unix_dates = ti.xcom_pull(key='unix_dates', task_ids='create_unix_dates_list')

    coin_infos_dict ={}
    for dicts in infos_json:
        if dicts['id'] in ids_list:
            coin_infos_dict[dicts['id']] = dicts

    fields = ['id','symbol','name']
    ids_dict = {}
    symbols_dict = {}
    names_dict = {}

    for coin_infos,coin in zip(coin_infos_dict.values(),ids_list): #this block will create lists of len(unix_dates)/len(ids_list) dimension to fit the dataframe
        ids_dict['id_list_to_col_'+coin] = [coin_infos_dict[coin]['id']]*int(len(unix_dates)/len(ids_list))
        symbols_dict['symbol_list_to_col_'+coin] = [coin_infos_dict[coin]['symbol']]*int(len(unix_dates)/len(ids_list))
        names_dict['name_list_to_col_'+coin] = [coin_infos_dict[coin]['name']]*int(len(unix_dates)/len(ids_list))
        
    infos = [ids_dict,symbols_dict,names_dict]

    ids_col = sum(ids_dict.values(), [])
    symbols_col = sum(symbols_dict.values(), [])
    names_col = sum(names_dict.values(), [])

    ti.xcom_push(key='ids_col', value=ids_col)
    ti.xcom_push(key='symbols_col', value=symbols_col)
    ti.xcom_push(key='names_col', value=names_col)

    return 

def create_load_df(ti):

    ids_col = ti.xcom_pull(key='ids_col', task_ids='create_infos_list')
    symbols_col = ti.xcom_pull(key='symbols_col', task_ids='create_infos_list')
    names_col = ti.xcom_pull(key='names_col', task_ids='create_infos_list')
    unix_dates = ti.xcom_pull(key='unix_dates', task_ids='create_unix_dates_list')
    final_columns_dict = ti.xcom_pull(key='final_columns_dict', task_ids='create_prices_list')

    df = pd.DataFrame()
    df['id'] = ids_col
    df['symbol'] = symbols_col
    df['name'] = names_col

    df['snapshot_date'] = unix_dates
    df['snapshot_date'] = df['snapshot_date'].apply(lambda x:datetime.utcfromtimestamp(x/1000).strftime('%Y-%m-%d'))

    for column,column_list in zip(final_columns_dict.keys(),final_columns_dict.values()):
        df['current_price_'+column[:3]] = column_list
        df['current_price_'+column[:3]] = df['current_price_'+column[:3]].apply(lambda x: round(x,2))



    credentials = service_account.Credentials.from_service_account_info(
	your_auth_credentials_dict
    )


    df.to_gbq(destination_table='your_table',
              project_id='your_project',
              if_exists='replace',
              credentials = credentials)

    return

    
with DAG('crypto_df_dag',start_date=days_ago(1),schedule_interval='0 10 * * *',catchup=False) as dag: # UTC -3 -> 7 = 10 - 3


    load_inputs_task = PythonOperator(
        task_id = 'load_inputs',
        python_callable = load_inputs
    )

    create_prices_urls_task = PythonOperator(
        task_id = 'create_prices_urls',
        python_callable = create_price_url
    )


    create_unix_dates_task = PythonOperator(
        task_id = 'create_unix_dates_list',
        python_callable = create_unix_dates
    )


    create_prices_list_task = PythonOperator(
        task_id = 'create_prices_list',
        python_callable = create_prices_list
    )


    create_infos_list_task = PythonOperator(
        task_id = 'create_infos_list',
        python_callable = create_infos_list
    )


    create_load_df_task = PythonOperator(
        task_id = 'create_load_df',
        python_callable = create_load_df
    )

load_inputs_task >> create_prices_urls_task >> create_unix_dates_task >> create_prices_list_task >> create_infos_list_task >> create_load_df_task
