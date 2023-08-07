import time
import hashlib
from datetime import datetime

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.weekday import WeekDay

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup


# from airflow.utils.log.logging_mixin import LoggingMixin
# class CustomHttpOperator(SimpleHttpOperator, LoggingMixin):
#     def execute(self, context):
#         print(self.endpoint)
#         return super().execute(context)


default_args = {
    'start_date': datetime(2023, 8, 1)
}

def _initialize(ti):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(str(time.time()).encode('utf-8'))
    HASH = str(sha256_hash.hexdigest())
    ti.xcom_push(key='DB_PATH',       value="/root/airflow/airflow.db")
    ti.xcom_push(key='TMP_DATA_PATH', value=f"/tmp/data-{HASH}.csv")

def _transform(ti):
    html   = ti.xcom_pull(task_ids='extract')
    bs_xml = BeautifulSoup(html, "html.parser")
    src    = bs_xml.find("tbody")
    src2   = src.get_text()

    cols   = {
        '경락일시': 'datetime',
        '도매시장': 'string', '법인': 'string', '부류': 'string', '품목': 'string', '품종': 'string', '출하지': 'string', '규격': 'string',
        '거래량': int, '경락가': float
    }

    values = np.array([d for d in src2.replace('\t', '').replace(',', '').split('\n') if d != ''])
    try:
        values = values.reshape(-1, len(cols))
    except ValueError:      # ValueError: cannot reshape array of size 1 into shape
        return 'finish'     # failure branch

    data = pd.DataFrame(values, columns=cols)
    for col, dtype in cols.items():
        if dtype == 'datetime':
            data[col] = pd.to_datetime(data[col])
        else:
            data[col] = data[col].astype(dtype)
    data.to_csv(ti.xcom_pull(key='TMP_DATA_PATH'), index=False, header=False)

    return 'load'  # success branch

with DAG(
    dag_id='agri-price-pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    tags=['agri'],
    catchup=True
) as dag:

    check_weekday = BranchDayOfWeekOperator(
        task_id='check_weekday',
        follow_task_ids_if_true='finish',
        follow_task_ids_if_false='initialize',
        week_day={WeekDay.SATURDAY, WeekDay.SUNDAY}
    )

    initialize = PythonOperator(
        task_id='initialize',
        python_callable=_initialize
    )

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id='db_sqlite',
        sql="""
            CREATE TABLE IF NOT EXISTS agri_price (
                경락일시    DATE NOT NULL,
                도매시장    TEXT NOT NULL,
                법인       TEXT NOT NULL,
                부류       TEXT NOT NULL,
                품목       TEXT NOT NULL,
                품종       TEXT NOT NULL,
                출하지     TEXT NOT NULL,
                규격       TEXT NOT NULL,
                거래량      INT NOT NULL,
                경락가      FLOAT NOT NULL
            )
        """
    )

    # extract = CustomHttpOperator(
    extract = SimpleHttpOperator(
        task_id='extract',
        endpoint='domeinfo/sanRealtime.do?pageNo=1&saledateBefore={{ execution_date | ds }}&largeCdBefore=&midCdBefore=&smallCdBefore=&saledate={{ execution_date | ds }}&whsalCd=&cmpCd=&sanCd=&smallCdSearch=&largeCd=08&midCd=01&smallCd=&pageSize=10',
        # endpoint='domeinfo/sanRealtime.do?pageNo=1&saledateBefore=2023-08-07&largeCdBefore=&midCdBefore=&smallCdBefore=&saledate=2023-08-07&whsalCd=&cmpCd=&sanCd=&smallCdSearch=&largeCd=08&midCd=01&smallCd=&pageSize=10',
        method='GET',
        http_conn_id='agri-price',
        log_response=True
    )

    transform = BranchPythonOperator(
        task_id='transform',
        python_callable=_transform
    )

    load = BashOperator(
        task_id='load',
        bash_command='echo -e ".separator ","\n.import {{ ti.xcom_pull(key="TMP_DATA_PATH") }} agri_price" | sqlite3 {{ ti.xcom_pull(key="DB_PATH") }}'
    )

    clean = BashOperator(
        task_id='clean',
        bash_command='rm {{ ti.xcom_pull(key="TMP_DATA_PATH") }}'
    )

    finish = BashOperator(
        task_id='finish',
        bash_command='echo -e "select * from agri_price" | sqlite3 {{ ti.xcom_pull(key="DB_PATH") }}',
        trigger_rule='one_success'
    )

    check_weekday >> [initialize, finish]
    initialize >> create_table >> extract >> transform >> [load, finish]
    transform >> load >> clean >> finish
