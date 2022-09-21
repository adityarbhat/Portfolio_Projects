# --------------------------------
# LIBRARIES
# --------------------------------

# Import Airflow Operators
from cgitb import text
from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
# Import Libraries to handle dataframes
import pandas as pd
#import numpy as np
import os
# Import Library to send Slack Messages
import requests
# Import Library to set DAG timing
from datetime import timedelta, datetime

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths
FILE_PATH_INPUT = '/home/airflow/gcs/data/input/'
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/output/'

# Import CSV into a pandas dataframe
file_name = os.listdir(FILE_PATH_INPUT)[0]
df = pd.read_csv(FILE_PATH_INPUT + file_name)

# Slack webhook link
slack_webhook = 'https://hooks.slack.com/services/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'

# --------------------------------
# FUNCTION DEFINITIONS
# --------------------------------

# Function to send messages over slack using the slack_webhook


def send_msg(text_string): requests.post(slack_webhook, json={'text': text_string})
   

# Function to generate the diagnostics report
# Add print statements to each variable so that it appears on the Logs
def send_report(df):
    """
  
    This function sends a disgnostics report to Slack with
    the below patient details:
    #1. Average O2 level
    #2. Average Heart Rate
    #3. Standard Deviation of O2 level
    #4. Standard Deviation of Heart Rate
    #5. Minimum O2 level
    #6. Minimum Heart Rate
    #7. Maximum O2 level
    #8. Maximum Heart Rate
    """
   
    #1. Average O2 level
    average_o2_level = df['o2_level'].mean()
    print('The average oxygen rate is {}'.format(average_o2_level))

    #2. Average Heart Rate
    average_heart_rate = df['heart_rate'].mean()
    print('The average heart rate is {}'.format(average_heart_rate) )

    #3. Standard Deviation of O2 level
    std_o2_level =  df['o2_level'].std()
    print('The Standard deviation of O2 level is {}'.format(std_o2_level))

    #4. Standard Deviation of Heart Rate
    std_heart_rate = df['heart_rate'].std()
    print('The Standard deviation of heart rate is {}'.format(std_heart_rate))

    #5. Minimum O2 level
    min_o2_level = df['o2_level'].min()
    print('Minimum O2 level is {}'.format(min_o2_level))

    #6. Minimum Heart Rate
    min_heart_rate = df['heart_rate'].min()
    print('Minimum heart rate is {}'.format(min_heart_rate))

    #7. Maximum O2 level
    max_o2_level = df['o2_level'].max()
    print('Maximum O2 level is {}'.format(max_o2_level))

    #8. Maximum Heart Rate
    max_heart_rate = df['heart_rate'].max()
    print('Maximum heart rate is {}'.format(max_heart_rate))
    
    
    text_string='''
    -----------------
    Diagnostic Report
    -----------------
    #1. Average O2 level {0}
    #2. Average Heart Rate {1}
    #3. Standard Deviation of O2 level {2}
    #4. Standard Deviation of Heart Rate {3}
    #5. Minimum O2 level {4}
    #6. Minimum Heart Rate {5}
    #7. Maximum O2 level {6}
    #8. Maximum Heart Rate {7}
    
    '''.format(average_o2_level,average_heart_rate,std_o2_level,std_heart_rate, min_o2_level, min_heart_rate,max_o2_level,max_heart_rate)

    send_msg(text_string)





#3 Function to filter anomalies in the data
# Add print statements to each output dataframe so that it appears on the Logs
def flag_anomaly(df):
    """
    As per the patient's past medical history, below are the mu and sigma 
    for normal levels of heart rate and o2:
    
    Heart Rate = (80.81, 10.28)
    O2 Level = (96.19, 1.69)

    Only consider the data points which exceed the (mu + 3*sigma) or (mu - 3*sigma) levels as anomalies
    Filter and save the anomalies as 2 dataframes in the output folder - hr_anomaly_P0015.csv & o2_anomaly_P0015.csv
    """
    heart_mu = 80.81
    heart_sigma = 10.28

    oxygen_mu = 96.19
    oxygen_sigma = 1.69

    #variables to hold the upper and lower anamoly values
    heart_upper = heart_mu + 3*heart_sigma
    heart_lower = heart_mu - 3*heart_sigma

    oxygen_upper = oxygen_mu + 3*oxygen_sigma
    oxygen_lower = oxygen_mu - 3*oxygen_sigma
    
    #filtering the anomalous values for heart rate and oxygen level and storing them as csv files 
    heart_anamolies_df = df[(df['heart_rate'] > heart_upper) | (df['heart_rate'] < heart_lower)]
    heart_anamolies_df.to_csv(f"{FILE_PATH_OUTPUT}/hr_anomaly_P0015.csv", index=False)

    print(heart_anamolies_df)

    oxygen_anamolies_df = df[(df['o2_level'] > oxygen_upper) | (df['o2_level'] < oxygen_lower)]
    oxygen_anamolies_df.to_csv(f"{FILE_PATH_OUTPUT}/o2_anomaly_P0015.csv", index=False)

    print(oxygen_anamolies_df)
    
# --------------------------------
# DAG definition
# --------------------------------

# Define the defualt args
default_args = {
    'owner': 'user_name',
    'start_date': datetime(2022, 9, 5),
    'depends_on_past': False,
    'email': ['aditya.greatlearning@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
# Create the DAG object

with DAG(
    'health_ops',
    default_args=default_args,
    description='DAG to monitor heartrate and O2 levels',
    schedule_interval='*/15 * * * *',
    catchup=False
) as dag:
    # start_task
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    ) 
    # file_sensor_task
    file_sensor = FileSensor(
        task_id='file_sensor',
        poke_interval=15,
        filepath=FILE_PATH_INPUT,
        timeout=5,
        dag=dag
    )
    # send_report_task
    send_slack_report = PythonOperator(
        task_id='send_slack_report',
        python_callable=send_report,
        dag=dag,
        op_kwargs={'df':df}
    )
    # flag_anomaly_task
    flag_anomaly_task=PythonOperator(
        task_id='flag_anomaly_task',
        python_callable=flag_anomaly,
        dag=dag,
        op_kwargs={'df':df}
    )
    # end_task
    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )
    
   
    
    
   
# Set the dependencies

# --------------------------------
start_task >> file_sensor >> [send_slack_report, flag_anomaly_task] >> end_task