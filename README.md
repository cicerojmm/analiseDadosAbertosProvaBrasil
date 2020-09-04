# Análise de Dados Abertos da Prova Brasil 2011

Importante: O projeto tem apenas a finalidade de estudar as ferramentas e servir como base de arquitetura para outras soluções.

A Prova Brasil é uma avaliação censitária das escolas públicas das redes municipais, estaduais e federal, com o objetivo de avaliar a qualidade do ensino. Participam desta avaliação as escolas que possuem, no mínimo, 20 alunos matriculados nas séries/anos avaliados, sendo os resultados disponibilizados por escola e por ente federativo.

Link dos dados abertos do Governo do Brasil: http://dados.gov.br/dataset/microdados-prova-brasil 

#### Stack Tecnológica: 
- Airflow: https://airflow.apache.org/
- Redshift: https://aws.amazon.com/pt/redshift/
- Amazon S3: https://aws.amazon.com/pt/s3/
- MetaBase: https://www.metabase.com/

#### Decisões arquiteturais
O Airflow foi escolhido para orquestrar a pipeline de dados por ser flexível e permitir organizar os passos com facilidades, caso necessite de escolabilidade pode facilmente incoporar outros frameworks de processamento distribuido. 
O Redshift é totalmente gerenciado e oferece uma solução completa para montar o DataWarehouse. 
O S3 permite armazenar os dados e carregar através dele as tabelas do Redshift, se tornando uma stagging area.
E o Metabase é uma solução de visualização de dados open source com recursos fáceis e eficientes para montar dashboards.
    
### Modelagem Dimensional dos dados

![alt text](https://github.com/cicerojmm/analiseDadosAbertosProvaBrasil/blob/master/images/modelagem-dimensional.png?raw=true)

### Execução do Projeto
O projeto está totalmente baseado no Docker e Docker Compose, basta seguir os passos abaixo:
1. Executar imagem do Airflow: 
```sh
$ docker-compose -f docker-compose-airflow.yml up -d
```
2. Executar imagem do Metabase: 
```sh
$ docker-compose -f docker-compose-metabase.yml up -d
```
2. Baixar os dados abertos: 
```sh
$ ./baixar_dados_abertos.sh
```
3. No painel web do Airflow cadastrar as connections do S3 com o nome *aws_s3* e do Redshift com o nome *aws_redshift*.
4. Criar as tabelas necessárias no Redshift utilizando o script *criacao-tabelas-dw-redshift.sql* deste repositório.
5. Criar um bucket no S3 e alterar o nome do bucket no código para o criado neste passo.
5. Executar a pipeline de dados no airflow.
6. Configurar e criar as visualizações no Metabase.

### Fluxo Completo no Airflow
![alt text](https://github.com/cicerojmm/analiseDadosAbertosProvaBrasil/blob/master/images/pipeline-completa-airflow.png?raw=true)

### Dashboard gerado no Metabase
![alt text](https://github.com/cicerojmm/analiseDadosAbertosProvaBrasil/blob/master/images/dashboard-metabase.png?raw=true)
