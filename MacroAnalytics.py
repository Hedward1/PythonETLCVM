# IMPORTANDO BASE DE DADOS
import pandas as pd
from sqlalchemy import create_engine


# Código feito pelo Andre Retorta
# Comentado por Hedward


# A Função abaixo vai fazer uma conexão com o link e recebe como parâmetro o nome para sabser de qual endereço na
# API vai consumir informações.
def consulta_bc(codigo_bc):
    """  """
    url = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json'.format(codigo_bc)
    df = pd.read_json(url)
    df['data'] = pd.to_datetime(df['data'], dayfirst=True)
    df.set_index('data', inplace=True)
    return df.loc['2003-01-01':'2023-12-01']


# Passa parametros para a função acima declarada(numeros dentro dos parenteses),


tx_juros_CDI = consulta_bc(12)
INPC = consulta_bc(188)
IGPM = consulta_bc(189)
IGPDI = consulta_bc(190)
IPCA = consulta_bc(433)
META_INFLACAO = consulta_bc(13521)
PIB_AGRO = consulta_bc(22083)
PIB_INDUSTRIA = consulta_bc(22084)
PIB_EXTRATIVA_MINERAL = consulta_bc(22085)
PIB_TRANSFORMACAO = consulta_bc(22086)
PIB_CONSTRUCAO_CIVIL = consulta_bc(22087)
PIB_ELETRE_GAS_AGUA = consulta_bc(22088)
PIB_SERVICOS = consulta_bc(22089)
PIB_COMERCIO = consulta_bc(22090)
PIB_TRANSPORTE = consulta_bc(22091)
PIB_SERVICOS_INFO = consulta_bc(22092)
PIB_ADM_SAUDE_EDUCA = consulta_bc(22093)
PIB_OUTROS_SERVICOS = consulta_bc(22094)
PIB_INTERMEDIACAO_FINANCEIRA = consulta_bc(22095)
PIB_ATIVIDADE_IMOBILIARIAS = consulta_bc(22096)
PIB_VALOR_ADICIONADO = consulta_bc(22097)
PIB_IMPOSTOS_PRODUTOS = consulta_bc(22098)
PIB_PRECO_MERCADO = consulta_bc(22099)
PIB_CONSUMO_FAMILIA = consulta_bc(22100)
PIB_CONSUMO_GOV = consulta_bc(22101)
PIB_EXP = consulta_bc(22103)
PIB_IMP = consulta_bc(22104)
PIB_CENTRO_OESTE = consulta_bc(25381)
PIB_NORDESTE = consulta_bc(25388)
PIB_SUDESTE = consulta_bc(25393)
PIB_SUL = consulta_bc(25400)
PIB_NORTE = consulta_bc(25406)
DESEMPREGO = consulta_bc(24369)
DOLAR = consulta_bc(1)
EURO = consulta_bc(21619)
LIBRA = consulta_bc(21623)
# ele realiza uma leitura sobre o Excel (usa a biblioteca pandas)
Populacao = pd.read_excel(r'C:\Users\sugui\Documents\Macro Analytics\Consolidado.xlsx')
PIB_PAIS = pd.read_excel(r'C:\Users\sugui\Documents\Macro Analytics\WEO_Data.xlsx')

# prepararando os dados com a visualisação que deseja
dataframe = [tx_juros_CDI, INPC, IGPM, IGPDI, IPCA, META_INFLACAO, PIB_AGRO, PIB_INDUSTRIA, PIB_EXTRATIVA_MINERAL,
             PIB_TRANSFORMACAO, PIB_CONSTRUCAO_CIVIL, PIB_ELETRE_GAS_AGUA, PIB_SERVICOS, PIB_COMERCIO, PIB_TRANSPORTE,
             PIB_SERVICOS_INFO, PIB_ADM_SAUDE_EDUCA, PIB_OUTROS_SERVICOS, PIB_INTERMEDIACAO_FINANCEIRA,
             PIB_ATIVIDADE_IMOBILIARIAS,
             PIB_VALOR_ADICIONADO, PIB_IMPOSTOS_PRODUTOS, PIB_PRECO_MERCADO, PIB_CONSUMO_FAMILIA, PIB_CONSUMO_GOV,
             PIB_EXP, PIB_IMP,
             PIB_CENTRO_OESTE, PIB_NORDESTE, PIB_SUDESTE, PIB_SUL, PIB_NORTE, DESEMPREGO, DOLAR, EURO, LIBRA]

# prepara a tabela para a visualizar os dados
df = pd.concat(dataframe, axis=1)

# criou as colunas com o nome desejado, como na variavel dataframe acima tx_juros_CDI(como conulta no json) e passou
# a ser somente CDI na coluna (no (BD))
df.columns = ['CDI', 'INPC', 'IGPM', 'IGPDI', 'IPCA', 'META INFLACAO', 'PIB AGRO', 'PIB INDUSTRIA',
              'PIB EXTRATIVO MINERAL', 'PIB TRANSFORMACAO', 'PIB CONSTRUCAO CIVIL',
              'PIB ELETRECIDADE GAS E AGUA', 'PIB SERVICOS', 'PIB COMERCIO', 'PIB TRANSPORTE',
              'PIB SERVICOES INFORMACOES', 'PIB ADM SAUDE E EDUCACAO', 'PIB OUTROS SERVICOS',
              'PIB INTERMEDIACAO FINANCEIRA', 'PIB ATIVIDADE IMOBILIARIAS', 'PIB VALOR ADICIONADO',
              'PIB IMPOSTOS PRODUTOS', 'PIB PRECO DE MERCADO', 'PIB CONSUMO FAMILIA',
              'PIB CONSUMO GOV', 'PIB_EXP', 'PIB_IMP', 'PIB CENTRO OESTE', 'PIB NORDESTE', 'PIB SUDESTE', 'PIB SUL',
              'PIB NORTE', 'DESEMPREGO', 'DOLAR', 'EURO',
              'LIBRA']
print(df)
print('Leitura Feita')

Serviodor = "FELIPE-LENOVO"
# A base de dados deve ser igual ao do SQL Server criada
BaseDeDados = "MACRO_ANALYTICS"
# Caso use um perfil para acessar o servidor (recomendável)
# se não tiver um perfil (ou ignorar o tutorial que passei na documentação, pode usar o código comentado abaixo,
# e comentar o segundo driver) pode usar o do próprio do windows
# Driver = "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=Yes"
Login = "Hedward"
Senha = "Familia1"
# normalmente o driver vai ser esse de baixo, levando-se em conta que é no SQL Server
Driver = "driver=ODBC+Driver+17+for+SQL+Server"

# Biblioteca de conexão com o banco de dados
engine = create_engine(f"mssql+pyodbc://{Login}:{Senha}@{Serviodor}/{BaseDeDados}?{Driver}")

print('Conectado com o Banco de Dados')

# ENVIANDO OS DADOS PARA BANCO DE DADOS

df.to_sql('Base de Dados', con=engine, if_exists='replace', index=True)
Populacao.to_sql('Populacao', con=engine, if_exists='replace', index=True)
PIB_PAIS.to_sql('PIB Pais', con=engine, if_exists='replace', index=True)

print('Dados enviados ao Banco')
