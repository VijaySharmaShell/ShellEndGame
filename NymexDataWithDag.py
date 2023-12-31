from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import task,dag
from MP2360Tools.MP2360Tools.utils import logz
import boto3
import csv
import pyodbc

logger = logz.create_logger()

s3 =  boto3.client('s3')
server = 'curves-uat.c3nmnzcia5f7.us-east-1.rds.amazonaws.com'
objC=s3.get_object(Bucket='curves-devenv', Key='config.txt')
dataC = objC['Body'].read().decode('utf-8').splitlines()
dataJson =  json.loads(dataC[0])

database = dataJson['database']
username = dataJson['username']
password = dataJson['password']
procedure = 'usp_setNymexData'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


args = {
    "owner": "Vijay",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure":""
}

@dag(
    dag_id="Nymex_Data",
    schedule_interval=timedelta(1),
    tags=["Pulling Nymex data from S3 to RDS"],
    default_args=args,
    catchup=False)




def callInsertData():
    @task
    def insertData():
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

       
        obj=s3.get_object(Bucket='curves-devenv', Key='feed.csv')

        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.reader(data)
        headers = next(records)
        headerCount = len(headers)

        xml = "<root>"
        for eachRecord in records:
            for count in range(0,headerCount): 
                xml+="<row><NymexCurve>"+eachRecord[count]+"</NymexCurve></row>"
        xml+="</root>"

        query = f"exec {procedure} '{xml}'"

        cursor.execute(query)
        conn.commit()

        cursor.close()
        conn.close()
    
    insertData()

dag=callInsertData()

