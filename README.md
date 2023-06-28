# Análise de Dados Abertos da Prova Brasil 2011

Este projeto tem como objetivo estudar ferramentas e servir como base de arquitetura para outras soluções.

A Prova Brasil é uma avaliação censitária das escolas públicas das redes municipais, estaduais e federal, com o objetivo de avaliar a qualidade do ensino. Participam dessa avaliação as escolas que possuem, no mínimo, 20 alunos matriculados nas séries/anos avaliados. Os resultados são disponibilizados por escola e por ente federativo.

Link para os dados abertos do Governo do Brasil: http://dados.gov.br/dataset/microdados-prova-brasil

## Stack Tecnológica
- Airflow: https://airflow.apache.org/
- Redshift: https://aws.amazon.com/pt/redshift/
- Amazon S3: https://aws.amazon.com/pt/s3/
- MetaBase: https://www.metabase.com/

## Decisões Arquiteturais
O Airflow foi escolhido para orquestrar a pipeline de dados devido à sua flexibilidade e facilidade de organização dos passos. Caso seja necessário, é possível incorporar facilmente outros frameworks de processamento distribuído para melhor escalabilidade. 
O Redshift é um serviço totalmente gerenciado que oferece uma solução completa para montar o Data Warehouse. 
O S3 permite armazenar os dados e carregar as tabelas no Redshift, atuando como uma área de preparação dos dados (staging area).
E o Metabase é uma solução de visualização de dados open source com recursos fáceis e eficientes para criar dashboards.

## Modelagem Dimensional dos Dados

![Modelagem Dimensional](https://github.com/cicerojmm/analiseDadosAbertosProvaBrasil/blob/master/images/modelagem-dimensional.png?raw=true)

## Execução do Projeto
Este projeto é baseado no Docker e Docker Compose. Siga as etapas abaixo:

1. Execute a imagem do Airflow: 
```sh
$ docker-compose -f docker-compose-airflow.yml up -d
```

2. Execute a imagem do Metabase: 
```sh
$ docker-compose -f docker-compose-metabase.yml up -d
```

3. Baixe os dados abertos: 
```sh
$ ./baixar_dados_abertos.sh
```

4. No painel web do Airflow, cadastre as conexões do S3 com o nome *aws_s3* e do Redshift com o nome *aws_redshift*.

5. Crie as tabelas necessárias no Redshift utilizando o script *criacao-tabelas-dw-redshift.sql* deste repositório.

6. Crie um bucket no S3 e altere o nome do bucket no código para o nome criado nesta etapa.

7. Execute a pipeline de dados no Airflow.

8. Configure e crie as visualizações no Metabase.

## Fluxo Completo no Airflow
![Pipeline Completa no Airflow](https://github.com/cicerojmm/analiseDadosAbertosProvaBrasil/blob/master/images/pipeline-completa-airflow.png?raw=true)

## Dashboard gerado no Metabase
![Dashboard no Metabase](https://github.com/cicerojmm/analiseDadosAbertosProvaBrasil/blob/master/images/dashboard-metabase.png?raw=true)