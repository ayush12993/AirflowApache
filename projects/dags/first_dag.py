try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All Dag modules are okay ....")
except Exception as e:
    print("Error e{}".format(e))


def first_function_execute(*args,**kwargs):
    variable=kwargs.get("name","Didn,t not get the key")
    print("Hello World ".format(variable))
    print("Comment")
    return "Hello World" + variable    

with DAG(
        dag_id="first_dag",
        schedule_interval="*/59 * * * *",
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
           op_kwargs={"name":"Soumil Shah"}
           )
