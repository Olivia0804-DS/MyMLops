from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.datasets import load_iris
import pandas as pd
import random
from datetime import datetime,timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'student',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

# Function to generate random training data for the Iris dataset
def generate_iris_data():
    iris = load_iris()
    data = iris['data']
    target = iris['target']
       
    # Create a DataFrame with Iris data
    df = pd.DataFrame(data, columns=iris['feature_names'])
    df['target'] = target
    
    # Randomly sample 100 rows
    sample_df = df.sample(n=100, replace=True, random_state=42)
    
    #log the DataFrame
    print("Generated Iris training data:")
    print(sample_df.head())

# Define the DAG
with DAG(
    dag_id='iris_training_data_generator',
    default_args=default_args,
    description='Generate training data for the Iris predictor every minute',
    start_date=datetime(2025, 1, 5),
    schedule='*/1 * * * *',  # Runs every minute
    catchup=False,
) as dag:
    
    # PythonOperator to generate Iris training data
    task = PythonOperator(
        task_id='generate_iris_training_data',
        python_callable=generate_iris_data,
    )


