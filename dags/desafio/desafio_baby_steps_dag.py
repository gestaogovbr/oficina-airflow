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

# Importe aqui a classe DAG
# Importe aqui a classe PostgresOperator
# Importe aqui a classe PythonOperator
# Importe aqui a classe PostgresHook

from datetime import datetime, timedelta


# Variables

# Aqui vai uma ajuda. Só apontar para o nome da conexão criada

PG_CONN_ID = '' # Inclua aqui o ID da conexão Postgres criada na oficina
template_searchpath = '/usr/local/airflow/dags/sql_query'

# Functions

# A função abaixo está incompleta. Pouquinha coisa!

def export_csv():
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    query = '' # Escreva aqui a query SQL para selecionar todos os dados da tabela uf_superficie_total
    fetch = pg_hook.get_pandas_df(query)
    fetch.to_csv(r'/usr/local/airflow/dags/export/uf_superficie_total.csv')

    return True

# DAG

# Defina os parâmetros da sua DAG

args = {
    # além do basicão do exercício anterior, inclua o parâmetro 
    # para mandar e-mail em caso de falha e o seu e-mail
    # documentação em: https://airflow.apache.org/tutorial.html?highlight=email_on_failure
    'owner': '', # preencher aqui
    'start_date': datetime(, , ), # Complete a data de início no formato (YYYY, MM, DD) com a data de anteontem
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'desafio_baby_steps_dag',
    default_args=args, # atualizar com a variável dos parâmetros definidos anteriormente
    template_searchpath=template_searchpath,
    schedule_interval='', # em formato cron: a cada hora, no minuto 0, apenas dias de semana. Gerador CRON: https://crontab-generator.org
    catchup=False # só estamos falando que a pizza é paulista. Brincadeira.. é pra não
                  # refazer o passado
)

# Tasks

query_task = PostgresOperator(
    task_id='', # nome único que irá aparecer na UI. Atenção: sem acentuo, espaço e
                # char especial
    postgres_conn_id=, # Inclua aqui a variável de conexão com o Postgres
    sql='', # Utilize o script SQL da pasta sql_query
    dag=dag
)

export_task = PythonOperator(
    task_id='', # nome único que irá aparecer na UI. Atenção: sem acentuo, espaço e
                # char especial
    python_callable=, # nome da função de exportação csv (linha 37)
    # confira se não está faltando mais nada aqui. A quem essa task pertence?
)

# Orchestration

# Crie abaixo a ordem de execução com base no nome das variáveis task

