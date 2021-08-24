#essential libraries
from datetime import datetime
#storing and analysis
import pandas as pd
from sqlalchemy import create_engine
from decouple import config 
PGSQL_USER=config('PGSQL_USER')
PGSQL_PASS=config('PGSQL_PASS')
PGSQL_HOST=config('PGSQL_HOST')
PGSQL_PORT=config('PGSQL_PORT')
PGSQL_BASE=config('PGSQL_BASE')
#hide warnings
import warnings
warnings.filterwarnings('ignore')

#função para tratar campo de datetime, recebe YY/MM/DD e convert para YYYY-MM-DD
def handleConvertToDate(old):
    return datetime.strftime(datetime.strptime(old, '%y/%m/%d'), '%Y-%m-%d')
	
#cria a conexão com banco de dados postgreSql
engine = create_engine(f"postgresql://{PGSQL_USER}:{PGSQL_PASS}@{PGSQL_HOST}:{PGSQL_PORT}/{PGSQL_BASE}")	

#variavel para montar string do arquivo csv
anomes = 202002

#leitura de arquivo csv
df = pd.read_csv('nome_arquivo_' + str(anomes) + '.csv',sep=';',encoding='ANSI')

#cabeçalho em letras minusculas, para padronizar o tratamento em colunas
df.columns = map(str.lower, df.columns)

#renomear algumas colunas padronizando o insert into para os proximos passos
df.rename(columns={'data_movimento':'data_movimento_old','ano_mes':'anomes'},inplace = True)

#criando nova coluna com aplicação da função handleConvertToDate
df['data_ativacao'] = df['data_movimento_old'].apply(handleConvertToDate)

#seleciono as colunas no formato correto para insert into no SQL
df = df[['anomes', 'info_01', 'data_ativacao', 'info_02', 'info_03','info_04','valor_contratado']]

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