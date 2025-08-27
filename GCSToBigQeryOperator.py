# Importing Modules
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests


# Database
MYSQL_CONNECTION = 'mysql_default'
# REST API
CONVERSION_RATE_URL = 'https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate'

# path data ใน airflow
mysql_output_path = '/home/airflow/gcs/data/audible_data_merged.csv'
conversion_rate_output_path = '/home/airflow/gcs/data/conversion_rate.csv'
final_output_path = '/home/airflow/gcs/data/output.csv'


# function data mysql 
def mysql_output_path(transaction_path):
    # รับ transaction_path จาก t1 

    # เชื่อมต่อ MySQL
    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    # query จาก database ใช้ hook ที่สร้าง จะได้ pandas DataFrame
    audible_data = mysqlserver.get_pandas_df(sql='SELECT * FROM audible_data')
    audible_transaction = mysqlserver.get_pandas_df(sql='SELECT * FROM audible_transaction')

    # รวม Merge audible_data และ audible_transaction
    data_book = audible_transaction.merge(audible_data, how='left', left_on='book_id', right_on='Book_ID')
 
    # save ไฟล์ csv transaction_path
    # '/home/airflow/gcs/data/audible_data_merged.csv'
    data_book.to_csv(transaction_path, index=False)
    print(f'Output to {transaction_path}')

# โหลดข้อมูลจาก REST API จาก url 
def get_conversion_rate(conversion_rate_path):
    # แปลง json เป็น dataframe
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    data_conversion_rate = pd.DataFrame(result_conversion_rate)

    # เปลี่ยน index เป็น data พร้อมชื่อคอลัมน์ว่า date
    data_conversion_rate = data_conversion_rate.reset_index().rename(columns={'index':'date'})
    data_conversion_rate.to_csv(conversion_rate_path, index=False)
    print(f'Output to {conversion_rate_path}')

def merge_data(transaction_path, conversion_rate_path, output_path):
    # ทำการอ่านไฟล์ทั้ง 3 ไฟล์ ที่โยนเข้ามาจาก task 
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)

    # สร้างคอลัมน์ใหม่ data ใน transaction
    # แปลง transaction['date'] เป็น timestamp
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # merge 2 datframe transaction, conversion_rate
    final_databook = transaction.merge(conversion_rate,
                                        how='left',
                                        left_on='date',
                                        right_on='date')
    
    # ลบเครื่องหมาย $ ในคอลัมน์ Price และแปลงเป็น float
    final_databook['Price'] = final_databook.apply(lambda x: x['Price'].replace('$',''), axis=1)
    final_databook['Price'] = final_databook['Price'].astype(float)

    # สร้างคอลัมน์ใหม่ชื่อว่า THBPrice เอา price * conversion_rate
    final_databook['THBPrice'] = final_databook['Price'] * final_databook['conversion_rate']
    final_databook = final_databook.drop(['date', 'book_id'], axis=1)

    # save ไฟล์ fianl_databook เป็น csv
    final_databook.to_csv(output_path, index=False)
    print(f"Output to {output_path}")

# Instantiate a Dag
with DAG(
    dat_id = 'gcs_to_gq_dag',
    start_data = dags_ago(1),
    schedule_interval = '@once',
    tags=["GCSToBigQueryOperator"]
) as dag:

    dag.doc_md = """ 
        # Load data to BigQuery by GCSToBigQueryOperator 
    """

    # Task
    # t1,t2,t3 ส่งค่าเข้าฟังก์ชันทำ trasform ข้อมูล
    t1 = PythonOperator(
        task_id = 'get_data_from_mysql',
        python_callable = get_data_from_mysql,
        op_kwargs = {
            'transaction_path' : mysql_output_path
        }
    )

    t2 = PythonOperator(
        task_id = 'get_conversion_rate',
        python_callable = get_conversion_rate,
        op_kwargs = {
            'conversion_rate_path' : conversion_rate_output_path
        }
    )

    t3 = PythonOperator(
        task_id = 'merge_data',
        python_callable = merge_data,
        op_kwargs = {
            'transaction_path' : mysql_output_path,
            'conversion_rate_path' : conversion_rate_output_path,
            'output_path' : final_output_path 
        }
    )

    # t4 ใช้ Airflow Operator
    t4 = GCSToBigQueryOperator(
        task_id = 'gcs_to_bq',
        bucket = 'asia-southeast1-storagebook-0feb77be-bucket', # link bucket
        source_objects = ['data/output.csv'],
        destination_project_dataset_table = 'marine-bay-288006.datawarehouse.book', # bigquery path
        skip_leading_rows = 1,
        schema_fields = [
            {
                'mode': 'NULLABLE',
                'name': 'timestamp',
                'type': 'TIMESTAMP'
            },
            {
                'mode': 'NULLABLE',
                'name': 'user_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'country',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Book_id',
                'type': 'INTEGER'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Book_Title',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Book_Subtitle',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Book_Narrator',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Audiobook_Type',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Categories',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Rating',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Total_No__of_Ratings',
                'type': 'FLOAT'
            },
            {
                'mode': 'NULLABLE',
                'name': 'Price',
                'type': 'FLOAT'
            },
            {
                'mode': 'NULLABLE',
                'name': 'conversion_rate',
                'type': 'FLOAT'
            },
            {
                'mode': 'NULLABLE',
                'name': 'THBPrice',
                'type': 'FLOAT'
            },
        ],
        write_disposition = 'WRITE_TRUNCATE'
    )

    # Setting up Dependencies
    [t1, t2] >> t3 >> t4