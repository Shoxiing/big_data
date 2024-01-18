from airflow.models import DAG
from typing import NoReturn
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging
import pickle
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests


dag = DAG('PARSER',
          schedule_interval="30 16 * * *",
          start_date=days_ago(2),
          catchup=False,
          tags=["Big_data"]
          )




_LOG = logging.getLogger()
_LOG.addHandler(logging.StreamHandler())


def init() -> NoReturn:
    _LOG.info('Parsing started')



def dai_zeny(origin, destination):
    response = requests.get("http://api.travelpayouts.com/aviasales/v3/get_latest_prices",
                            params = {'origin': origin,
                                      'destination' : destination,
                                      'one_way':True,
                                      'period_type':'year',
                                      'limit': '999',
                                      'sorting': 'price',
                                      'token':'0ab204bee152e0e81783f7ac0f488766'})
    if not response.ok:
        print('Server responded:', response.status_code)
    else:
        infa = response.json()
    return infa



def cheap (origin):
    cities = ['KJA','MOW','LED', 'BAX', 'ASF', 'KZN', 'KGD','ABA', 'VOZ', 'UUS', 'SVX', 'KRR', 'AYT', 'VIE','DXB' ]
    dep_cities = []
    for i in cities:
        if i == origin:
            cities.remove(i)
    for i in cities:
        zena = dai_zeny(origin,i)
        dep_cities.append(zena)
    return dep_cities


BUCKET = 'mlds-de'
DATA_PATH = 'datasets/data_1.pkl'

def parsing_s():

    list_dates_1 = cheap('MOW')
    list_dates_2 = cheap('KJA')
    list_dates_3 = cheap('LED')
    list_dates_4 = cheap('BAX')
    list_dates_5 = cheap('ASF')
    list_dates_6 = cheap('KZN')
    list_dates_7 = cheap('KGD')
    list_dates_8 = cheap('ABA')
    list_dates_9 = cheap('VOZ')
    list_dates_10 = cheap('UUS')
    list_dates_11 = cheap('SVX')
    list_dates_12 = cheap('KRR')
    list_dates_13 = cheap('AYT')
    list_dates_14 = cheap('VIE')
    list_dates_15 = cheap('DXB')

    data = list_dates_1 + \
             list_dates_2 + \
             list_dates_3 + \
             list_dates_4 + \
             list_dates_5 + \
             list_dates_6 + \
             list_dates_7 + \
             list_dates_8 + \
             list_dates_9 + \
             list_dates_10 + list_dates_11 + list_dates_12 + list_dates_13 + list_dates_14 + list_dates_15

    s3_hook = S3Hook('s3_cn')
    session = s3_hook.get_session('ru-central1')
    resource = session.resource('s3')

    pickle_save = pickle.dumps(data)
    resource.Object(BUCKET, DATA_PATH).put(Body=pickle_save)


task_init = PythonOperator(task_id='init', python_callable=init, dag=dag)

task_parsing_s = PythonOperator(task_id='parsing_s', python_callable=parsing_s, dag=dag)

task_init >> task_parsing_s