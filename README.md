# projectPython-01
project data processing

Neste Exemplo: 
  faço a leitura de um arquivo csv, 
  conexão com postgreSql utilizando SQLALCHEMY, 
  tratamento de colunas,
  renomeio colunas para padrozinação,
  tratamento de datetime,
  seleciono apenas as colunas na ordem que preciso para inserir os dados na tabela SQL,
  delete na tabela SQL com a Chave de anomes se ja existe aquele padrão no exemplo 202002,
  como já tenho a tabela, faço um df.to_sql com a propriedade if_exists='append', caso não tenha a tabela criada
    retire essa propriedade que o próprio comando do Python criará uma tabela como o que colocar na propriedade name='tabela',
  retorno simples para verificar quantos anomes tenho na tabela e quantidade de linhas para cada um deles.





# INICIO
======

#essential libraries

from datetime import datetime

#storing and analysis

import pandas as pd

from sqlalchemy import create_engine

#hide warnings

import warnings

warnings.filterwarnings('ignore')


#função para tratar campo de datetime, recebe YY/MM/DD e convert para YYYY-MM-DD

def handleConvertToDate(old):
    return datetime.strftime(datetime.strptime(old, '%y/%m/%d'), '%Y-%m-%d')
	
#cria a conexão com banco de dados postgreSql

engine = create_engine('postgresql://usuario:senha@localhost:5432/database')	

#variavel para montar string do arquivo csv

anomes = 202002

#leitura de arquivo csv

df = pd.read_csv('arquivo_' + str(anomes) + '.csv',sep=';',encoding='ANSI')
#df.columns

#cabeçalho em letras minusculas, para padronizar o tratamento em colunas

df.columns = map(str.lower, df.columns)

#renomear algumas colunas padronizando o insert into para os proximos passos

df.rename(columns={'data_movimento':'data_movimento_old','valor_contrado':'valor_contratado','ano_mes':'anomes'},inplace = True)

#criando nova coluna com aplicação da função handleConvertToDate

df['data_ativacao'] = df['data_movimento_old'].apply(handleConvertToDate)

#seleciono as colunas no formato correto para insert into no SQL

df = df[['anomes', 'info_01', 'data_ativacao', 'info_02', 'info_03',
       'info_04','valor_contratado']]

#delete da tabela sql se já exist o anomes do conteudo em questao

delete_string='DELETE FROM tabela where anomes = %s'
engine.execute(delete_string,[anomes]) 

#insere os dados na tabela

df.to_sql(
    name='tabela',
    con=engine,
    index=False, 
    if_exists='append'
)
print("dados inseridos, anomes: " + str(anomes))

 
#Faz uma leitura agrupada por anomes da tabela:
#Aqui podemos fazer tambem queries complexas para retornar e darmos continuidade em plotagem e graficos.		

#retorno simples para verificar quantos anomes tenho na tabela e quantidade de linhas para cada um deles

res = pd.read_sql_query( "select anomes, count(*) as tt from tabela group by anomes",engine)
res

# FIM

