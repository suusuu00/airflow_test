import datetime
import pendulum
import pandas as pd
import numpy as np
import os

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import MinMaxScaler
import joblib

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import os
import io

# 현재 스크립트의 디렉토리를 가져옴
current_dir = os.path.dirname(__file__)
top_dir = os.path.dirname(current_dir)

# joblib 파일의 절대 경로 생성
joblib_path = os.path.join(top_dir, 'models', 'preprocessing_pipeline.joblib')
joblib_path2 = os.path.join(top_dir, 'models', 'preprocessing_pipeline_second_category.joblib')


def process_file(**kwargs):
    # 업로드된 파일 객체를 XCom으로부터 받아옴
    uploaded_file = kwargs['ti'].xcom_pull(task_ids='upload_task', key='uploaded_file')

    # 업로드된 파일 객체의 내용을 Pandas DataFrame으로 읽어 처리하는 로직을 수행
    dataframe = pd.read_csv(io.BytesIO(uploaded_file.read()))

    # 여기에서 Pandas DataFrame을 활용한 추가적인 처리 로직을 수행

    print("업로드된 파일의 내용을 Pandas DataFrame으로 처리했습니다.")
    kwargs['ti'].xcom_push(key='processed_dataframe', value=dataframe)


def remove_id_column(X):
    return X.drop(columns = 'ID', axis=1)

def dropna_function(X):
    return X.dropna()

def label_encode_column(X):

    le = LabelEncoder()
    X = pd.DataFrame(le.fit_transform(X), columns=X.columns)
    
    return X

pipe = joblib.load(joblib_path)
category_pipe = joblib.load(joblib_path2)

def preprocessing(**kwargs):

    new_df = kwargs['ti'].xcom_pull(task_ids='process_file_task', key='processed_dataframe')
    
    log_cols = ['Age', 'Work_Experience', 'Family_Size']
    binary_cols = ['Gender', 'Ever_Married', 'Graduated']

    X_transformed = pd.DataFrame(pipe.transform(new_df))
    category_cols_name = category_pipe.get_feature_names_out()
    X_transformed.columns = log_cols + binary_cols + list(category_cols_name)
    
    print(X_transformed)

'''
def preprocessing_callable(**kwargs):
    # 여기에서 kwargs로 받은 데이터 처리
    new_df = kwargs.get('dag_run').conf.get('new_df')
    if new_df:
        preprocessing(pd.DataFrame(new_df))
'''        
    
with DAG(
    dag_id="customer_data_preprocessing_operator",
    schedule="0 0 * * *",       # 분 시 일 월 요일
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    process_file_task = PythonOperator(task_id='process_file_task',
                provide_context=True,
                python_callable=process_file,
            )
    
    preprocess_task = PythonOperator(task_id='preprocess_task',
                    provide_context=True,
                    python_callable=preprocessing,
                    
                    )
    
    process_file_task >> preprocess_task