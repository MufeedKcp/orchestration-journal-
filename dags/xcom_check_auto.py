from airflow.sdk import dag, task


@dag(
    dag_id="xcom_check_auto"
)

def xcom_check_auto():

    @task
    def extract_data():
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task 
    def transform_data(data: dict):
        fetched_data = data['data']
        transformed_data = fetched_data * 2
        transformed_data_dict = {"transformed_data": transformed_data}
        return transformed_data_dict

    @task 
    def load_data(data: dict):
        loaded_data = data
        return loaded_data


    task_1 = extract_data()
    task_2 = transform_data(task_1)
    task_3 = load_data(task_2)

xcom_check_dag = xcom_check_auto()