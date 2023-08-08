# Airflow-DAGs
Storage For Airflow DAGs

# 1. [nft-pipeline.py](https://github.com/alchemine/airflow-dags/blob/main/nft-pipeline.py)
[OpenSea](https://opensea.io)에 올라온 NFT 중 **Doodles** 데이터에 대한 ETL pipeline 

```
creating_table >> is_api_available >> extract_nft >> process_nft >> store_nft
```


# 2. [agri-price-pipeline](https://github.com/alchemine/airflow-dags/blob/main/agri-price-pipeline.py)
[aT도매시장 통합홈페이지](https://at.agromarket.kr)에 올라오는 **실시간 경매현황** 중 **수박** 데이터에 대한 ETL pipeline 

```
check_weekday >> [initialize, finish]
initialize >> create_table >> extract >> transform >> [load, finish]
transform >> load >> clean >> finish
```

## - Point
1. 날짜 별 분기 설정(`BranchDayOfWeekOperator`)
2. Transform → Load 과정에서 저장되는 임시 데이터 경로명을 위한 hash값을 xcom으로 DAG 내부에서 공유
   - Q. 데이터를 저장하지 않고 `@task`를 사용해도 되긴하지만, operator를 쓰고 싶다. 같이 쓸 수 있는 fancy method는 없을까?
3. 데이터 유효성 검사를 통한 분기 설정(`BranchPythonOperator`)
4. Jinja를 이용한 `execution_date` 사용
   - `{{ execution_date | ds }}`
   - ref. https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
5. Bash command를 이용한 csv file import
    - `bash_command='echo -e ".separator ","\n.import {{ ti.xcom_pull(key="TMP_DATA_PATH") }} agri_price" | sqlite3 {{ ti.xcom_pull(key="DB_PATH") }}'`
    - Q. IDENTITY column은 어떻게?
6. `initialize` task에서 xcom variable을 추가
    ```
    HASH = ... 
    ti.xcom_push(key='DB_PATH',       value="/root/airflow/airflow.db")
    ti.xcom_push(key='TMP_DATA_PATH', value=f"/tmp/data-{HASH}.csv")
    ```
7. `initialize >> extract >> transform >> load >> clean >> finish` pipeline은 꽤 완성도가 있는 것 같다.
