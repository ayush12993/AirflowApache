try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All Dag modules are okay ....")
except Exception as e:
    print("Error e{}".format(e))


def first_function_execute(**context):
    variable=kwargs.get("first_function_execute ")
    context['t1'].xcom_push(key='mykey',value="first_function_execute says Hello")

def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    print("I am in second_function_execute got value :{} from Function 1 ".format(instance))



with DAG(
        dag_id="first_dag",
        schedule_interval="*/* * * * *",
        default_args={
           "owner": "airflow",
           "retries": 1,
           "retry_delay": timedelta(minutes=5),
           "start_date": datetime(2022, 11 ,30)
            },
        catchup=False) as f:
       
    
    first_function_execute = PythonOperator(
           task_id="first_function_execute",
           python_callable=first_function_execute,
           provide_context=True,
           op_kwargs={"name":"Ayush Jain"},
           )

    second_function_execute = PythonOperator(
           task_id="second_function_execute",
           python_callable=second_function_execute,
           provide_context=True,
           )

first_function_execute >> second_function_execute


