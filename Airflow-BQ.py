from airflow import DAG
import pendulum

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

PROJE_AD = "massive-hub-423119-c8"
DB_AD = "Bitirme_projesi2"

dag = DAG(
    dag_id="18_GCSToBigQuery",
    schedule="@daily",
    start_date=pendulum.datetime(2023,5,31,tz="UTC")
    )

sorgu =f"Select * from {PROJE_AD}.{DB_AD}.mqtt where service = 'mqtt'"
    
create_new_table = BigQueryExecuteQueryOperator(
        task_id = "create_new_table",
        sql=sorgu,
        destination_dataset_table=f"{PROJE_AD}.{DB_AD}.butun_veri_analiz_proje",
        create_disposition="CREATE_IF_NEEDED", 
        write_disposition="WRITE_APPEND",#WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        dag=dag
        
)


create_new_table