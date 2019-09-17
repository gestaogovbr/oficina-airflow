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
    'owner': '', # preencher aqui
    'start_date': airflow.utils.dates.days_ago(2), # aka anteontem
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'desafio_hello_world_dag',
    default_args='', # atualizar com a variável dos parâmetros definidos anteriormente
    schedule_interval='', # em formato cron: toda segunda às 09:30
                          # ajuda em https://crontab-generator.org/
    catchup=False # só estamos falando que a pizza é paulista. Brincadeira.. é pra não
                  # refazer o passado
)

# Tasks

hello_task = BashOperator(
    task_id='', # nome único que irá aparecer na UI. Atenção: sem acentuo, espaço e
                # char especial
    bash_command='', # substituir por print("Hello"), só que no formato bash
    dag=dag
)


world_task = BashOperator(
    task_id='', # mesma orientação que task anterior
    bash_command='', # substituir por print("World!"), só que no formato bash
    # confira se não está faltando mais nada aqui. A quem essa task pertence?
)

# Orchestration

# atualizar ordem de execução com base no nome das variáveis task

alguma_variavel >> vem_antes_da_outra
