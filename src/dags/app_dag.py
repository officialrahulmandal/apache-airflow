from data_retrival import helloWorld, secondWorld
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

SCHEDULE_INTERVAL = '@hourly'

default_args = {
    'owner' : 'Business Intelligence',
    'depends_on_posts': False,
    'start_date': datetime(2019, 1, 16),
    'email_on_failure' : 'True',
    'email_on_retry' : False,
    'retries':3,
    'retry_delay': timedelta(minutes=5)
}

DAG_VERSION = 'MyApp.0'

dag = DAG(DAG_VERSION
, default_args=default_args
, schedule_interval = SCHEDULE_INTERVAL
, concurrency = 1,
max_active_runs=1
)

# first_task = PythonOperator(
#     task_id='first_app',
#     python_callable=helloWorld,
#     retries=0,
#     dag=dag
# )

# second_task = PythonOperator(
#     task_id='second_app',
#     python_callable=secondWorld,
#     retries=0,
#     dag=dag
# )


# first_task >> second_task

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)
dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

t1 >> [t2, t3]