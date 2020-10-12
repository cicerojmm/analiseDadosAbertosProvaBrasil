#!/bin/bash
wget ftp://ftp.inep.gov.br/microdados/microdados_prova_brasil_2011.zip
unzip microdados_prova_brasil_2011.zip
mkdir dags/dados
mv Microdados\ Prova\ Brasil\ 2011/Dados/TS_RESULTADO_ALUNO.csv dags/dados/
mv Microdados\ Prova\ Brasil\ 2011/Dados/TS_RESULTADO_MUNICIPIO.csv dags/dados/
rm -rf Microdados\ Prova\ Brasil\ 2011/
rm -rf microdados_prova_brasil_2011.zip
