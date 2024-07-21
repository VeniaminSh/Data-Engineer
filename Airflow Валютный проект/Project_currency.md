# Проект: отправка нынешнего курса доллара ЦБ на почту

Импорт библитоек для Dag:
```
#код Dag
import requests
import xml.etree.ElementTree as ET
import logging
from datetime import date, datetime, timedelta

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email_smtp
```

В аирфлоу необходимо загнать конекшн на постгрес с названием post_con, обращаться будем к локальной бд за пределами докера.
```
Connection Id = post_con
host = host.docker.internal
Database = qwe
Login = postgres
Port = 5432
```

Через хук вытянем движок, с помощью которого будут исполняться запросы на постгре, дата исполнения дага и адресатов - куда будут отправлены письма.
```
#код Dag
p_con = 'post_con'
postgres_hook = PostgresHook(p_con)
engine = postgres_hook.get_sqlalchemy_engine()

#переменная дата исполнения дага
business_dt = '{{ ds }}'

#адресаты
MAIL_LIST = ['v.shendogan@gmail.com', 'amira.ar@yandex.ru']
```
Напишем сами функции, которые будут добалять данные в таблицу.

Cобираем таблицу, куда будем складывать данные с сайта Центробанка, манипуляции с SEQUENCE (для корректного значения в поле id), при вставке новых данных в таблицу показатель id опирается на последнее вставленое значение в поле id, если оно равно Null, то встанет 1, см в функцию insert_in_table переменная tr
```
def pricol():
    tra = '''CREATE TABLE IF NOT EXISTS public.tra (id INT8 NOT NULL, 
                                       texts VARCHAR NOT NULL, 
                                       dates timestamp NOT NULL,
                                       sums float4 NOT NULL);
             DROP SEQUENCE IF EXISTS public.tra_id_seq;
             CREATE SEQUENCE public.tra_id_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE;'''
    engine.execute(tra)
```

Вытаскиваем данные с XLM, нынешнего курса доллара в рублях, и кладем в таблицу postgres.
```
def insert_in_table(ti, date):
    #вытаскиваем данные только за нынешнее число

    #приводим дату к необходимому формату
    date = datetime.strptime(date, '%Y-%m-%d')
    date_for_url = date.strftime('%d/%m/%Y')
    
    #переменная с динамической датой (дата выполнения дага)
    url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={str(date_for_url)}'
    
    #выводим страничку xml
    response = requests.get(url)
    xml_data = response.content
    root = ET.fromstring(xml_data)

    #вытаскиваем из xml доллар - код записи R01235
    desired_valute = root.find(".//Valute[@ID='R01235']")
    #вытягиваем курс доллара
    value = round(float(desired_valute.find('Value').text.replace(',','.')),2)
    #валютное обозначение
    exchange = desired_valute.find('CharCode').text
    date_insert = date.strftime('%Y-%m-%d')
    #первое поле для таблицы в посгресе
    texts = "'На сегодняшний день, курс одного, " + exchange + " составляет " + str(value) + " рублей'"

    #первой строкой формируем идемподентность и вставляем данные в таблицу
    tra = f"DELETE FROM public.tra WHERE dates = '{date_insert}'; \
        INSERT INTO public.tra (id, texts, dates, sums) VALUES (setval('tra_id_seq', COALESCE((SELECT MAX(id) FROM tra), 0) + 1, false) ,{texts}, '{date_insert}', '{str(value)}');"
    engine.execute(tra)
    
    #для функции отправки сообщения в почту необходимо перенести некоторые переменные
    ti.xcom_push(key='texts', value=texts)
    ti.xcom_push(key='dates', value=date_insert)
```

Отправляем сообщение 
```
def not_e(ti):
    title = 'Заголовок)'
    
    body = f'''{ti.xcom_pull(key='texts')} дата {ti.xcom_pull(key='dates')} '''
    send_email_smtp(";".join(MAIL_LIST), title, body)
```

Формат результа 
> 'На сегодняшний день, курс одного, USD составляет 88.02 рублей' дата 2024-07-21


Дописываем оставшуюся часть дага.
```
args = {
    "owner": 'veny',
    'email': ['v.shendogan@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0
}

with DAG(
        'tra',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup= True,
        start_date= days_ago(1)
) as dag:
    first = PythonOperator(
        task_id='f',
        python_callable=pricol)
    
    second = PythonOperator(
        task_id='f2',
        python_callable=insert_in_table,
        op_kwargs = {'date': business_dt})
    
    third = PythonOperator(
        task_id='f3',
        python_callable=not_e)
    
first >> second >> third
```
