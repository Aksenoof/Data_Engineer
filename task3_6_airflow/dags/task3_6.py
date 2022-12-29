
from datetime import datetime
import psycopg2

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor

from airflow.models import TaskInstance

from airflow.hooks.base import BaseHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from random import randrange
import re
import os.path


def get_connect_credentials(connect_id) -> BaseHook.get_connection:
    connect_to_airflow = BaseHook.get_connection(connect_id)
    return connect_to_airflow

def hello():
    print('Airflow')

# PythonOperator, который генерирует два произвольных числа и печатает их
def generator():
    num1 = randrange(100)
    num2 = randrange(100)
    if os.path.isfile('/opt/airflow/generator_num.txt'):
        with open('/opt/airflow/generator_num.txt', 'r') as file:
            lines = file.readlines()
            lines = lines[:-1]
        with open('/opt/airflow/generator_num.txt', 'w') as file:
            file.writelines(lines)  
        with open('/opt/airflow/generator_num.txt', 'a') as file:
            file.write('%d, %d ' % (num1, num2))
    else:
        with open('/opt/airflow/generator_num.txt', 'a') as file:
            file.write('%d, %d ' % (num1, num2))        
    
    with open('/opt/airflow/generator_num.txt', 'r') as file:
        print(file.read())

# разность сумм кол. 1 и кол. 2
def calc():
    with open('/opt/airflow/generator_num.txt', 'r') as file:
        z1 = list()
        z2 = list()
        for line in file:
            if line.strip():
                y = [int(i) for i in re.findall(r'\b\d+\b', line)]
                z1.append(y[0])
                z2.append(y[1])
        sum1 = sum(z1)
        sum2 = sum(z2)

    result = sum1 - sum2

    with open('/opt/airflow/generator_num.txt', 'a') as file:
        file.write('\n %d' % result)
    
    with open('/opt/airflow/generator_num.txt', 'r') as file:
        print(file.read())


# Sensor-оператор 
def check(**kwargs):
    # проверка на существование файла
    file_exists = os.path.isfile('/opt/airflow/generator_num.txt')
    print(file_exists) 
   
    # количество строк в файле минус одна последняя - соответствует кол-ву запусков  
    # создаем файл для запись кол-ва запусков
    if os.path.isfile('/opt/airflow/check_count.txt'):
        with open('/opt/airflow/check_count.txt', 'r') as file:
            count = int(file.read())
    else:
        count = 0
    
    ti = TaskInstance(check_task, kwargs['execution_date'])
    state = ti.current_state()
    # запись кол-ва запусков 
    if state == 'running':
        count += 1
        with open('/opt/airflow/check_count.txt', '+w') as file:
            file.write('%d' % count)  

    # количество строк в файле     
    sum_lines = sum(1 for line in open('/opt/airflow/generator_num.txt'))
    print(count, (sum_lines - 1)) 

    # Финальное число рассчитано верно
    with open('/opt/airflow/generator_num.txt', 'r') as file:
            lines = file.readlines()
            result_from_file = int(lines[-1])
            lines1 = lines[:-1]
            z1 = list()
            z2 = list()
            for line in lines1:
                if line.strip():
                    y = [int(i) for i in re.findall(r'\b\d+\b', line)]
                    z1.append(y[0])
                    z2.append(y[1])
            sum1 = sum(z1)
            sum2 = sum(z2)
    result_calc = sum1 - sum2
    print(result_from_file, result_calc)
    
    if file_exists and ((sum_lines - 1) == count) and (result_from_file == result_calc):
        return True
    else:
        return False

# оператор вейтвления
def python_branch():
    file_exists = os.path.isfile('/opt/airflow/generator_num.txt')
    if file_exists:
        file_empty = os.stat('/opt/airflow/generator_num.txt').st_size
    else:
        file_empty = 0
    
    if file_exists and (file_empty > 0):
        return 'load_in_postgres'
    else:
        return 'no_load_in_postgres'

# работа psql
def work_psql(**kwargs):
    ti = kwargs['ti']

    connect_id = Variable.get("connect_id")
    connect_to_airflow = get_connect_credentials(connect_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = connect_to_airflow.host, connect_to_airflow.port,\
                                                             connect_to_airflow.login, connect_to_airflow.password,\
                                                                 connect_to_airflow.schema
    
    ti.xcom_push(value = [pg_hostname, pg_port, pg_username, pg_pass, pg_db], key='connect_to_airflow')
    pg_connect = psycopg2.connect(host = pg_hostname, port = pg_port, user = pg_username, password = pg_pass, database = pg_db)

    cursor = pg_connect.cursor()

    with open('/opt/airflow/generator_num.txt', 'r') as file:
        lines = file.readlines()
        line = [int(i) for i in re.findall(r'\b\d+\b', lines[-2])]

    cursor.execute("CREATE TABLE IF NOT EXISTS calc_table (id serial PRIMARY KEY, col1 integer, col2 integer, diff_sum_col integer);")
    cursor.execute("INSERT INTO calc_table (col1, col2) VALUES (%s, %s)", line)
    pg_connect.commit()

    cursor.execute("SELECT (SUM(col1) - SUM(col2)) FROM calc_table")
    num = cursor.fetchone()
    print(num)

    cursor.execute("SELECT id FROM calc_table ORDER BY id DESC LIMIT 1")
    id = cursor.fetchone()
    print(id)
    
    cursor.execute("UPDATE calc_table SET diff_sum_col = %s WHERE id = %s", (num, id))
    pg_connect.commit()
    
    cursor.close()
    pg_connect.close()



with DAG(dag_id = 'task3_6', start_date = datetime(2022, 12, 8), schedule = '1-5 18 * * *') as dag:

    accurate = DummyOperator(
        task_id = 'load_in_postgres'
    )
    inaccurate = DummyOperator(
        task_id = 'no_load_in_postgres'
    )

    bash_task = BashOperator(task_id = 'hello', bash_command = 'echo hello', do_xcom_push=False)
    python_task = PythonOperator(task_id = 'world', python_callable = hello, do_xcom_push=False)
    generator_task = PythonOperator(task_id = 'generator_num', python_callable = generator, do_xcom_push=False)
    calc_task = PythonOperator(task_id = 'calc_num', python_callable = calc, do_xcom_push=False)

    check_task = PythonSensor(
        task_id = 'check',
        python_callable = check,
        timeout = 0,
        soft_fail = False,
        dag = dag)
    
    branch_task = BranchPythonOperator(
        task_id = 'branch_oper',
        python_callable = python_branch,
        do_xcom_push = False)

    work_psql_task = PythonOperator(task_id = 'work_psql', python_callable = work_psql, do_xcom_push=False)
    bash_error_task = BashOperator(task_id='Error', bash_command='echo file not found or empty', do_xcom_push=False) 

bash_task >> python_task >> generator_task >> calc_task >> check_task >> \
branch_task >> accurate >> work_psql_task 
branch_task >> inaccurate >> bash_error_task