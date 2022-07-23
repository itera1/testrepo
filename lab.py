from urllib import request
remote_url = ' https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
# Define the local filename to save data
local_file = '/home/project/airflow/dags/finalassignment/staging/tolldata.tgz'
# Download remote and save locally
request.urlretrieve(remote_url, local_file)

#import libraries
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import tarfile
import csv
#Exercise 2 - Create a DAG
#Task 1.2 - Define the DAG
#define DAG arguments
default_args={
    'owner':'Yuliia Ihnatova',
    'start_date':days_ago(0),
    'email':['yuliia.ihnatova@kneu.ua'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}
#DAG definition
dag=DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1)
)

#define the tasks
#task 1.3
def unzip_data_task():
    file = tarfile.open('/home/project/airflow/dags/finalassignment/staging/tolldata.tgz')
    file.extractall('/home/project/airflow/dags/finalassignment/staging')
    file.close()
    with open ('/home/project/airflow/dags/finalassignment/staging/fileformats.txt', 'r') as f:
        content=f.readlines()
        print(content)

unzip_data=PythonOperator(task_id='unzip_data', python_callable=unzip_data_task, dag=dag) 
#task 1.4   
def extract_data_from_csv_task():
    with open('/home/project/airflow/dags/finalassignment/staging/vehicle-data.csv') as f:
        reader = csv.reader(f)
        data=list(reader)
        Rowid =[]
        Timestamp=[]
        Anonymized_Vehicle_number=[]
        Vehicle_type=[]
        for line in data: 
            Rowid.append(line[0])
            Timestamp.append(line[1])
            Anonymized_Vehicle_number.append(line[2])
            Vehicle_type.append(line[3])
        rows=zip(Rowid, Timestamp, Anonymized_Vehicle_number,Vehicle_type)
        headers=['Rowid', 'Timestamp', 'Anonymized_Vehicle_number','Vehicle_type']
        with open('/home/project/airflow/dags/finalassignment/staging/csv_data.csv', 'w') as f:
            writer=csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                 writer.writerow(row)

extract_data_from_csv=PythonOperator(task_id='extract_data_from_csv', python_callable=extract_data_from_csv_task, dag=dag) 

#task 1.5

def  extract_data_from_tsv_task():
    with open("/home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv") as f:
        tsv_file = csv.reader(f, delimiter="\t")
        data=list(tsv_file)
        Number_of_axles=[]
        Tollplaza_id=[] 
        Tollplaza_code=[]
        for line in data:
            Number_of_axles.append(line[4])
            Tollplaza_id.append(line[5])
            Tollplaza_code.append(line[6])
        rows=zip(Number_of_axles, Tollplaza_id, Tollplaza_code )
        headers=['Number_of_axles', 'Tollplaza_id', 'Tollplaza_code' ]
        with open('/home/project/airflow/dags/finalassignment/staging/tsv_data.csv', 'w') as f:
            writer=csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)   

extract_data_from_tsv=PythonOperator(task_id='extract_data_from_tsv', python_callable=extract_data_from_tsv_task, dag=dag) 

#task 1.6
def extract_data_from_fixed_width_task():
    with open('/home/project/airflow/dags/finalassignment/staging/payment-data.txt','r') as f:
        data=f.readlines() 
        Type_of_Payment_code = [] 
        Vehicle_Code=[]
        for line in data:
            Type_of_Payment_code.append(line[-10:-7])
            Vehicle_Code.append(line[-6:-1])
        rows=zip(Type_of_Payment_code, Vehicle_Code)
        headers=['Type_of_Payment_code', 'Vehicle_Code']  
        with open('/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv', 'w') as f:
            writer=csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row) 

extract_data_from_fixed_width=PythonOperator(task_id='extract_data_from_fixed_width', python_callable=extract_data_from_fixed_width_task, dag=dag)

#task 1.7 

bash_task="paste '/home/project/airflow/dags/finalassignment/staging/csv_data.csv' '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv'  '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv'  >  '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv'"
consolidate_data=BashOperator(task_id='consolidate_data', bash_command=bash_task, dag=dag)          
#task 1.8
def transform():
    with open ('/home/project/airflow/dags/finalassignment/staging/extracted_data.csv','r') as f:
        lines =f.read()
        d = { "car": "CAR", "van": "VAN",'truck': 'TRUCK'}
        for key, value in d.items():
            lines = lines.replace(key, value)
            with open('/home/project/airflow/dags/finalassignment/staging/transformed_data.csv', 'w') as out:
                out.write(lines)  

transform_data=PythonOperator(task_id='transform_data', python_callable=transform, dag=dag)                                          
#create pipeline
unzip_data>>extract_data_from_csv
extract_data_from_csv>>extract_data_from_tsv
extract_data_from_tsv>>extract_data_from_fixed_width
extract_data_from_fixed_width>>consolidate_data
consolidate_data>>transform_data