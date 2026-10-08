from airflow.sdk import dag, task


# --- Define Sales Data ---
# Representing sales data for 12 months (or any 12 periods)
SALES_DATA_1_YEAR = [
    12000, 15000, 18000, 20000, 14000, 16000,
    22000, 25000, 17000, 19000, 21000, 23000
]

@dag(
    dag_id="xcom_check_sales_data"

)

def xcom_check_sales_data():    

    @task
    def push_sales_data(**kwargs):

        # pushing the SALES_DATA_1_YEAR to xcom for the next task to use
        print(f"Pushing {len(SALES_DATA_1_YEAR)} month sales data to XCom.")
        return SALES_DATA_1_YEAR

    @task   
    def aggregate_sales_data(**kwargs):
        ti = kwargs.get('ti')
        # pulling the sales data from the `push_sales_data` task to use it in the current task
        sales_array = ti.xcom_pull(task_ids="push_sales_data")

        if not sales_array:
            print("No sales data found in XCom.")
            return None
        
            # 2. Aggregate and Calculate Average
        total_sales = sum(sales_array)
        num_months = len(sales_array)
        average_sales = total_sales / num_months

            # 3. Print Results
        print(f"\n--- Aggregation Results ---")
        print(f"Total Sales (Sum): ${total_sales:,.2f}")
        print(f"Number of Periods: {num_months}")
        print(f"Average Sales: **${average_sales:,.2f}**")

    push_task = push_sales_data()
    aggregate_task = aggregate_sales_data()

    push_task >> aggregate_task

xcom_check_sales_data_dag = xcom_check_sales_data()
    
