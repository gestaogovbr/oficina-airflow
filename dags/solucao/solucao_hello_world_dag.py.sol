# Olá! Seja bem vindo à sua primeira DAG. O Airflow é uma ferramenta
# de orquestração muito versátil e torcemos que essa amizade (você e o Airflow)
# seja sólida e duradoura. Quanto mais vocês se conhecerem, mais perceberá
# o volume de trabalho que ele está disposto a fazer por você, seu time e o
# Brasil.
#
# Esses são os votos de Nitai e Vitor.

# Libraries

# Mamata. Tudo que você vai precisar já está importado

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

# DAG

# Aqui começa a brincadeira. Defina os parâmetros da sua DAG

args = {
    'owner': 'ATIs_do_ME', # preencher aqui
    'start_date': airflow.utils.dates.days_ago(2), # aka anteontem
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'solucao_hello_world_dag',
    default_args=args, # atualizar com a variável dos parâmetros definidos anteriormente
    schedule_interval='30 9 * * 1', # em formato cron: toda segunda às 09:30
    catchup=False # só estamos falando que a pizza é paulista. Brincadeira.. é pra não
                  # refazer o passado
)

# Tasks

hello_task = BashOperator(
    task_id='hello_task', # nome único que irá aparecer na UI. Atenção: sem acentuo, espaço e
                          # char especial
    bash_command='echo "Hello"', # substituir por print("Hello"), só que no formato bash
    dag=dag
)


world_task = BashOperator(
    task_id='world_task', # mesma orientação que task anterior
    bash_command='echo "World!"', # substituir por print("World!"), só que no formato bash
    # confira se não está faltando mais nada aqui. A quem essa task pertence?
    dag=dag
)

# Orchestration

# atualizar ordem de execução com base no nome das variáveis task

hello_task >> world_task

