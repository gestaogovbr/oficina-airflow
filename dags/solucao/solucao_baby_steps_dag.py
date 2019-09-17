# Olá! Seja bem vindo à sua segunda DAG.
#
# A primeira foi só para introduzir os pombinhos sem muito trauma.
# Nesse baby steps vamos fazer um exercício simplificado que já pode
# resolver desafios da vida real.
# Não se esqueça que o oceano de oportunidades é vasto e profundo
# e a proposta deste workshop é apenas mostrar que não é tão difícil
# assim começar com um barquinho. Afinal, Amyr Klink atravessou o Atlântico
# a remo em 100 dias. (https://www.goodreads.com/book/show/1575430.Cem_Dias_Entre_C_u_e_Mar)
#
# Fé na luta!
# Inté. Nitai e Vitor.

# Libraries

# Agora vai ter que se virar fião e fiona

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta

# Variables

# Aqui vai uma ajuda. Só apontar para o nome da conexão criada

PG_CONN_ID = 'db_oficina'
template_searchpath = '/usr/local/airflow/dags/sql_query'

# Functions

def export_csv():
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    query = 'SELECT * FROM uf_superficie_total'
    fetch = pg_hook.get_pandas_df(query)
    fetch.to_csv(r'/usr/local/airflow/dags/export/uf_superficie_total_indigenas.csv')

    return True

# DAG

# Defina os parâmetros da sua DAG

args = {
    # além do basicão do exercício anterior coloque
    # para mandar e-mail em caso de falha e o seu e-mail
    # documentação em: https://airflow.apache.org/tutorial.html?highlight=email_on_failure
    'owner': 'ATIs_do_ME', # preencher aqui
    'start_date': datetime(2019, 9, 15),
    'depends_on_past': False,
    'retries': 0,
    'email': ['atis_do_me@economia.gov.br'], # atenção: esse e-mail não existe
    'email_on_failure': True
}

dag = DAG(
    'solucao_baby_steps_dag',
    default_args=args, # atualizar com a variável dos parâmetros definidos anteriormente
    template_searchpath=template_searchpath,
    schedule_interval='0 * * * 1-5', # em formato cron: a cada hora, no minuto 0, apenas dias de semana
    catchup=False # só estamos falando que a pizza é paulista. Brincadeira.. é pra não
                  # refazer o passado
)

# Tasks

query_task = PostgresOperator(
    task_id='query_task',
    postgres_conn_id=PG_CONN_ID,
    sql='solucao_query_uf_superficie_total.sql',
    dag=dag
)

export_task = PythonOperator(
    task_id='export_csv_task',
    python_callable=export_csv,
    dag=dag
)

# Orchestration

# atualizar ordem de execução com base no nome das variáveis task

query_task >> export_task
