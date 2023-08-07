from datetime import datetime
import json
from pandas import json_normalize

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'start_date': datetime(2023, 1, 1),
}

def _processing_nft(ti):
    data = ti.xcom_pull(task_ids='extract_nft')
    if data is None:
        raise ValueError("data is empty")

    assets         = data['assets'][0]
    asset_contract = assets['asset_contract']

    processed_nft = json_normalize({
        'token_id': assets['token_id'],
        'name': asset_contract['name'],
        'image_url': asset_contract['image_url']
    })
    processed_nft.to_csv('/tmp/processed_nft.csv', index=False, header=False)


with DAG(
    dag_id='nft-pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    tags=['nft'],
    catchup=True
) as dag:

    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE IF NOT EXISTS nfts (
                token_id    TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                image_url   TEXT NOT NULL
            )
        '''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collection=doodles-official&limit=1',
    )

    extract_nft = SimpleHttpOperator(
        task_id='extract_nft',
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collection=doodles-official&limit=1',
        method='GET',
        response_filter=lambda res: json.loads(res.text),
        log_response=True
    )

    process_nft = PythonOperator(
        task_id='process_nft',
        python_callable=_processing_nft,
    )

    store_nft = BashOperator(
        task_id='store_nft',
        bash_command='echo -e ".separator ","\n.import /tmp/processed_nft.csv nfts" | sqlite3 /root/airflow/airflow.db'
    )

    creating_table >> is_api_available >> extract_nft >> process_nft >> store_nft
