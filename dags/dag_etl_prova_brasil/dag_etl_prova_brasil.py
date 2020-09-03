from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from dag_etl_prova_brasil.etl_prova_brasil import ETLProvaBrasil, TABELAS

etl_prova_brasil = ETLProvaBrasil()

default_args = {
    'owner': 'cicerojmm',
    'start_date': datetime(2020, 9, 1),
    'depends_on_past': False,
    'retries': 3
}

dag_prova_brasil = DAG(
    'dag_etl_prova_brasil',
    default_args=default_args,
    description='Dag para realizar ETL dos dados abertos da Prova Brasil 2011',
    schedule_interval=None
)

etl_dim_localizacao = PythonOperator(
    task_id='etl_dim_localizacao',
    python_callable=etl_prova_brasil.extrair_dados_localizacao,
    dag=dag_prova_brasil)

etl_dim_escola = PythonOperator(
    task_id='etl_dim_escola',
    python_callable=etl_prova_brasil.extrair_dados_escola,
    dag=dag_prova_brasil)

etl_dim_turma = PythonOperator(
    task_id='etl_dim_turma',
    python_callable=etl_prova_brasil.extrair_dados_turma,
    dag=dag_prova_brasil)

etl_fato_resultado_aluno = PythonOperator(
    task_id='etl_fato_resultado_aluno',
    python_callable=etl_prova_brasil.extrair_dados_resultado_aluno,
    dag=dag_prova_brasil)

envia_arquivos_s3 = []
for tabela, arquivo in TABELAS.items():
    envia_arquivos_s3.append (
        PythonOperator(
            task_id='envia_arquivo_s3_{}'.format(tabela),
            op_kwargs={'tabela': tabela, 'arquivo': arquivo},
            python_callable=etl_prova_brasil.salvar_dados_s3,
            dag=dag_prova_brasil
        )
    )

etl_dim_localizacao >> etl_dim_escola >> etl_dim_turma >> \
    etl_fato_resultado_aluno >> envia_arquivos_s3


carrega_tabelas_dw = []
for tabela, arquivo in TABELAS.items():
    carrega_tabelas_dw.append(
        PythonOperator(
            task_id='carrega_tabela_redshift_{}'.format(tabela),
            op_kwargs={'tabela': tabela, 'arquivo': arquivo},
            python_callable=etl_prova_brasil.gravar_dados_redshift,
            dag=dag_prova_brasil
        )
    )
    if len(carrega_tabelas_dw) > 1:
        carrega_tabelas_dw[-2] >> carrega_tabelas_dw[-1]
    else:
        envia_arquivos_s3 >> carrega_tabelas_dw[0]

carrega_tabela_fato = PythonOperator(
    task_id='carrega_tabela_redshift_FATO_RESULTADO_ALUNO',
    python_callable=etl_prova_brasil.carregar_tabela_fato,
    dag=dag_prova_brasil)

deleta_arquivos_carga = BashOperator(
    task_id='deleta_arquivos_carga',
    bash_command='rm -rf /usr/local/airflow/dags/dados/*',
    dag=dag_prova_brasil
)

carrega_tabelas_dw[-1] >> carrega_tabela_fato >> deleta_arquivos_carga
