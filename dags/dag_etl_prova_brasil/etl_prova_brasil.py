import pandas as pd
import numpy as np
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

from dag_etl_prova_brasil.util_prova_brasil import UtilProvaBrasil

TABELAS = {
    'DIM_LOCALIZACAO': 'dim_localizacao.csv',
    'DIM_ESCOLA': 'dim_escola.csv',
    'DIM_TURMA': 'dim_turma.csv',
    'ODS_RESULTADO_ALUNO': 'ods_resultado_aluno.csv'
}


class ETLProvaBrasil():

    def __init__(self):
        self.redshift_hook = PostgresHook(postgres_conn_id='aws_redshift')
        self.s3_client = S3Hook(aws_conn_id='aws_s3')
        self.path_pasta_dados = '/usr/local/airflow/dags/dados/'
        self.bucket_name = 'prova-brasil'

    def get_dados_principais(self):
        return pd.read_csv(self.path_pasta_dados + 'TS_RESULTADO_ALUNO.csv', delimiter=';')

    def extrair_dados_localizacao(self):
        df_localiazacao = pd.read_csv(
            self.path_pasta_dados + 'TS_RESULTADO_MUNICIPIO.csv', delimiter=';')
        df_loc_filtrado = df_localiazacao[[
            'ID_UF', 'SIGLA_UF', 'ID_MUNICIPIO', 'NOME_MUNICIPIO']]
        df_loc_agrupado = df_loc_filtrado.drop_duplicates().reset_index().drop('index', axis=1)
        df_loc_agrupado = df_loc_agrupado[[
            'ID_UF', 'ID_MUNICIPIO', 'SIGLA_UF', 'NOME_MUNICIPIO']]
        df_loc_agrupado.to_csv(self.path_pasta_dados + 'dim_localizacao.csv')

    def extrair_dados_turma(self):
        util = UtilProvaBrasil()
        df = self.get_dados_principais()
        df_turma = df[['ID_TURNO', 'ID_SERIE']]

        df_turma['ID_TURNO'].replace(' ', np.nan, inplace=True)
        df_turma = df_turma.dropna()
        df_turma = df_turma.astype('int64')

        df_turma['DESC_SERIE'] = df_turma.apply(
            util.get_descricao_serie, axis=1)
        df_turma['DESC_TURNO'] = df_turma.apply(
            util.get_descricao_turno, axis=1)

        df_turma = df_turma.drop_duplicates().reset_index().drop('index', axis=1)
        df_turma = df_turma[['ID_SERIE',
                             'ID_TURNO', 'DESC_SERIE', 'DESC_TURNO']]
        df_turma.to_csv(self.path_pasta_dados + 'dim_turma.csv')

    def extrair_dados_escola(self):
        util = UtilProvaBrasil()
        df = self.get_dados_principais()
        df_escola = df[['ID_ESCOLA', 'ID_DEPENDENCIA_ADM', 'ID_LOCALIZACAO']]
        df_escola = df_escola.dropna()
        df_escola['DESC_DEPENDENCIA_ADM'] = df_escola.apply(
            util.get_descricao_dependencia_adm, axis=1)
        df_escola['DESC_LOCALIZACAO'] = df_escola.apply(
            util.get_descricao_localizacao, axis=1)
        df_escola = df_escola.drop_duplicates().reset_index().drop('index', axis=1)
        df_escola = df_escola[['ID_ESCOLA',
                               'DESC_DEPENDENCIA_ADM', 'DESC_LOCALIZACAO']]
        df_escola.to_csv(self.path_pasta_dados + 'dim_escola.csv')

    def extrair_dados_resultado_aluno(self):
        df = self.get_dados_principais()
        df_fato = df[['ID_PROVA_BRASIL', 'ID_UF', 'ID_MUNICIPIO', 'ID_ESCOLA', 'ID_TURNO', 'ID_SERIE',
                      'PROFICIENCIA_LP_SAEB', 'PROFICIENCIA_MT_SAEB', 'IN_SITUACAO_CENSO', 'IN_PREENCHIMENTO', 'IN_PROFICIENCIA']]
        df_fato_filtrada = df_fato[(df_fato['IN_SITUACAO_CENSO'] == 1) & (
            df_fato['IN_PREENCHIMENTO'] == 1) & (df_fato['IN_PROFICIENCIA'] == 1)]

        df_fato_filtrada = df[['ID_PROVA_BRASIL', 'ID_UF', 'ID_MUNICIPIO', 'ID_ESCOLA', 'ID_TURNO', 'ID_SERIE',
                               'PROFICIENCIA_LP_SAEB', 'PROFICIENCIA_MT_SAEB']]
        df_fato_filtrada['PROFICIENCIA_LP_SAEB'].replace(
            ' ', np.nan, inplace=True)
        df_fato_filtrada['PROFICIENCIA_MT_SAEB'].replace(
            ' ', np.nan, inplace=True)
        df_fato_filtrada['ID_TURNO'].replace(' ', np.nan, inplace=True)
        df_fato_filtrada = df_fato_filtrada.dropna()

        df_fato_filtrada['PROFICIENCIA_LP_SAEB'] = df_fato_filtrada['PROFICIENCIA_LP_SAEB'].apply(
            lambda x: x.replace(',', '.')).astype('float64').round(2)
        df_fato_filtrada['PROFICIENCIA_MT_SAEB'] = df_fato_filtrada['PROFICIENCIA_MT_SAEB'].apply(
            lambda x: x.replace(',', '.')).astype('float64').round(2)

        df_fato_filtrada.to_csv(
            self.path_pasta_dados + 'ods_resultado_aluno.csv', index=False)
    
    def carregar_tabela_fato(self):
        conn = self.redshift_hook.get_conn()
        cursor = conn.cursor()
        load_statement = """
            insert
                into
                fat_resultado_aluno (
                select
                    escola.sk_escola, turma.sk_turma, loc.sk_localizacao, id_prova_brasil, 
                    ods.proficiencia_lp_saeb, ods.proficiencia_mt_saeb, 
                    ((ods.proficiencia_lp_saeb + ods.proficiencia_mt_saeb) / 2)
                from
                    ods_resultado_aluno ods
                inner join dim_escola escola on
                    ods.id_escola = escola.cd_escola
                inner join dim_turma turma on
                    (ods.id_serie = turma.cod_serie
                    and ods.id_turno = turma.cod_turno)
                inner join dim_localizacao loc on
                    (ods.id_uf = loc.cd_uf
                    and ods.id_municipio = loc.cd_municipio) )
            """
        cursor.execute(load_statement)
        cursor.close()
        conn.commit()
        return True

    def salvar_dados_s3(self, tabela, arquivo):
        self.s3_client.load_file(
            self.path_pasta_dados + arquivo, bucket_name=self.bucket_name, key=arquivo)

    def gravar_dados_redshift(self, tabela, arquivo, delimiter=',', region='us-east-2'):
        conn = self.redshift_hook.get_conn()
        cursor = conn.cursor()
        load_statement = """
            copy
            {0}
            from 's3://{1}/{2}'
            iam_role 'arn:aws:iam::042634135812:role/access_redshift_s3'
            csv
            ignoreheader 1
            delimiter '{3}' region '{4}' 
            """.format(
            tabela, self.bucket_name, arquivo,
            delimiter, region)

        cursor.execute(load_statement)
        cursor.close()
        conn.commit()
        return True
