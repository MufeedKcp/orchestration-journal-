from airflow.sdk import dag, task


@dag(
    dag_id="xcom_check_manual"
)

def xcom_check_manual():

    @task
    def extract_data(**kwargs):
        # extracting 'ti' from kwargs to push xcom value manually
        ti = kwargs.get('ti')
        print("Extracting data...")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        # pushing xcom value manually
        ti.xcom_push(key="fetched_data", value=fetched_data)

    @task 
    def transform_data(**kwargs):
        ti = kwargs.get('ti')
        print("Transforming data...")
        # pulling the xcom value from the previous task to use it in the current task
        fetched_data = ti.xcom_pull(task_ids="extract_data", key="fetched_data")

        transformed_data = fetched_data["data"] * 2
        transformed_data_dict = {"transformed_data": transformed_data}

        # pushing the transformed data to xcom for the next task to use
        ti.xcom_push(key="transformed_data", value=transformed_data_dict)

    @task
    def load_data(**kwargs):
        ti = kwargs.get('ti')
        print("Loading data...")
        loaded_data = ti.xcom_pull(task_ids="transform_data", key="transformed_data")
        return loaded_data


    task_1 = extract_data()
    task_2 = transform_data(task_1)
    task_3 = load_data(task_2)

xcom_check_dag = xcom_check_manual()