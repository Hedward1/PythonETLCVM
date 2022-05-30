from sqlalchemy import create_engine  # biblioteca de conexão com SQL Server
import pandas as pd  # Biblioteca usada para tratar os dados
import zipfile  #
import io  # Melhora a flexiblidade de leitura e escrita de arquivos em conjunto do zipfile
import requests as requests  # conexção com link

# Lista dos anos fazer Download, quanto mais anos, mais demorado (acredito que nas datas atuais deve durar em torno de
# 6 – 7 horas de processo para deixar pronto no banco de dados).
# Quando tem [] significa é uma lista, no python pode ser texto, numeros, e tudo mais guardado na lista
listaData = ["2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"]

# listaData = ["2017", "2018", "2019", "2020", "2021", "2022"]

DFP = True
ITR = True

# Conexão com Banco usando a biblioteca SQLAlchemy
# voce deve colocar o seu servidor, no meu caso é:
Serviodor = "FELIPE-LENOVO"

# A base de dados deve ser igual ao do SQL Server criada
BaseDeDados = "CVM_DW_CIAS"

# Caso use um perfil para acessar o servidor (recomendável)
# se não tiver um perfil (ou ignorar o tutorial que passei na documentação, pode usar o código comentado abaixo,
# e comentar o segundo driver) pode usar o do próprio do windows
# Driver = "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=Yes"
# normalmente o driver vai ser esse de baixo, levando-se em conta que é no SQL Server
Driver = "driver=ODBC+Driver+17+for+SQL+Server"
Login = "Hedward"
Senha = "xxx"

# Biblioteca de conexão com o banco de dados
engine = create_engine(f"mssql+pyodbc://{Login}:{Senha}@{Serviodor}/{BaseDeDados}?{Driver}")
eng_connection = engine.connect()

# mostra na tela o inicio do código
print('Iniciando valores no ITR')
# primeira parte do código vai preparar as ITRs para o banco de dados
# a função len() Retorna o número total de itens na lista
# a função range() Retorna o valor entre 0 e x, mas se eu quiser, pode ser entre 200 e x
# lendo fica da seguinte forma:
# para cada VALOR(X) contido num INTERALO(range) com um TOTAL(len) contado da LISTA(listaData) deve fazer....

if ITR:
    for x in range(len(listaData)):
        # mostra em que ano processa as informações, simplemente para saber o quanto falta
        print(f'indo para o ano {listaData[x]} performatico')

        # noinspection PyBroadException
        try:
            link = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_{listaData[x]}.zip'

            # Declarado de forma especificar cada variável vai receber qual documento de dentro da
            # ligação(link) que possue uma compressão de ficheiros(zip)

            # As variaveis só vão receber a String por agora, essa String vai servir como referência para receber
            # arquivo do zipfile
            ABERTA_YEAR = 'itr_cia_aberta_' + listaData[x] + '.csv'
            BPA_CON_YEAR = 'itr_cia_aberta_BPA_con_' + listaData[x] + '.csv'
            BPA_IND_YEAR = 'itr_cia_aberta_BPA_ind_' + listaData[x] + '.csv'
            BPP_CON_YEAR = 'itr_cia_aberta_BPP_con_' + listaData[x] + '.csv'
            BPP_IND_YEAR = 'itr_cia_aberta_BPP_ind_' + listaData[x] + '.csv'
            DFC_MI_CON_YEAR = 'itr_cia_aberta_DFC_MI_con_' + listaData[x] + '.csv'
            DFC_MI_IND_YEAR = 'itr_cia_aberta_DFC_MI_ind_' + listaData[x] + '.csv'
            DRE_CON_YEAR = 'itr_cia_aberta_DRE_con_' + listaData[x] + '.csv'
            DRE_IND_YEAR = 'itr_cia_aberta_DRE_ind_' + listaData[x] + '.csv'

            # Acessa os dados do link

            r = requests.get(link)

            # Com a biblioteca Zip é possivel visualizar(em memória RAM) os dados sem ter de baixar eles
            # e salva essas informações na variável zf

            zf = zipfile.ZipFile(io.BytesIO(r.content))

            # já declarada no códico de bloco salva cada arquivo CVS que está no arquivo compactado
            # para uma nova variável

            zf_ABERTA_YEAR = zf.open(ABERTA_YEAR)
            zf_BPA_CON_YEAR = zf.open(BPA_CON_YEAR)
            zf_BPA_IND_YEAR = zf.open(BPA_IND_YEAR)
            zf_BPP_CON_YEAR = zf.open(BPP_CON_YEAR)
            zf_BPP_IND_YEAR = zf.open(BPP_IND_YEAR)
            zf_DFC_MI_CON_YEAR = zf.open(DFC_MI_CON_YEAR)
            zf_DFC_MI_IND_YEAR = zf.open(DFC_MI_IND_YEAR)
            zf_DRE_CON_YEAR = zf.open(DRE_CON_YEAR)
            zf_DRE_IND_YEAR = zf.open(DRE_IND_YEAR)

            # realizará a leitura nas linhas para futura tratativa

            l_ABERTA_YEAR = zf_ABERTA_YEAR.readlines()
            l_BPA_CON_YEAR = zf_BPA_CON_YEAR.readlines()
            l_BPA_IND_YEAR = zf_BPA_IND_YEAR.readlines()
            l_BPP_CON_YEAR = zf_BPP_CON_YEAR.readlines()
            l_BPP_IND_YEAR = zf_BPP_IND_YEAR.readlines()
            l_DFC_MI_CON_YEAR = zf_DFC_MI_CON_YEAR.readlines()
            l_DFC_MI_IND_YEAR = zf_DFC_MI_IND_YEAR.readlines()
            l_DRE_CON_YEAR = zf_DRE_CON_YEAR.readlines()
            l_DRE_IND_YEAR = zf_DRE_IND_YEAR.readlines()

            # tratativa de informações que possuem caracteres especiais como Ç ou ~ entre outros

            l_ABERTA_YEAR = [i.strip().decode('ISO-8859-1') for i in l_ABERTA_YEAR]
            l_BPA_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPA_CON_YEAR]
            l_BPA_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPA_IND_YEAR]
            l_BPP_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPP_CON_YEAR]
            l_BPP_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPP_IND_YEAR]
            l_DFC_MI_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DFC_MI_CON_YEAR]
            l_DFC_MI_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DFC_MI_IND_YEAR]
            l_DRE_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DRE_CON_YEAR]
            l_DRE_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DRE_IND_YEAR]

            # Arquivo CSV normalmente separa os valores(células) por ";", é isso que estamos organizando abaixo

            l_ABERTA_YEAR = [i.split(';') for i in l_ABERTA_YEAR]
            l_BPA_CON_YEAR = [i.split(';') for i in l_BPA_CON_YEAR]
            l_BPA_IND_YEAR = [i.split(';') for i in l_BPA_IND_YEAR]
            l_BPP_CON_YEAR = [i.split(';') for i in l_BPP_CON_YEAR]
            l_BPP_IND_YEAR = [i.split(';') for i in l_BPP_IND_YEAR]
            l_DFC_MI_CON_YEAR = [i.split(';') for i in l_DFC_MI_CON_YEAR]
            l_DFC_MI_IND_YEAR = [i.split(';') for i in l_DFC_MI_IND_YEAR]
            l_DRE_CON_YEAR = [i.split(';') for i in l_DRE_CON_YEAR]
            l_DRE_IND_YEAR = [i.split(';') for i in l_DRE_IND_YEAR]

            # Aqui é de fato já usando a biblioteca pandas(DataFrame) para visualisação.
            # Se apenas escrever o nome da variável é possível visualizar as informações PRONTAS
            # df_ABERTA_YEAR

            df_ABERTA_YEAR = pd.DataFrame(l_ABERTA_YEAR[1:], columns=l_ABERTA_YEAR[0])
            df_BPA_CON_YEAR = pd.DataFrame(l_BPA_CON_YEAR[1:], columns=l_BPA_CON_YEAR[0])
            df_BPA_IND_YEAR = pd.DataFrame(l_BPA_IND_YEAR[1:], columns=l_BPA_IND_YEAR[0])
            df_BPP_CON_YEAR = pd.DataFrame(l_BPP_CON_YEAR[1:], columns=l_BPP_CON_YEAR[0])
            df_BPP_IND_YEAR = pd.DataFrame(l_BPP_IND_YEAR[1:], columns=l_BPP_IND_YEAR[0])
            df_DFC_MI_CON_YEAR = pd.DataFrame(l_DFC_MI_CON_YEAR[1:], columns=l_DFC_MI_CON_YEAR[0])
            df_DFC_MI_IND_YEAR = pd.DataFrame(l_DFC_MI_IND_YEAR[1:], columns=l_DFC_MI_IND_YEAR[0])
            df_DRE_CON_YEAR = pd.DataFrame(l_DRE_CON_YEAR[1:], columns=l_DRE_CON_YEAR[0])
            df_DRE_IND_YEAR = pd.DataFrame(l_DRE_IND_YEAR[1:], columns=l_DRE_IND_YEAR[0])

            # Junta as informações uma abaixo da outra

            # NOVO = MAIS PERFORMÁTICO
            BPA = pd.concat((df_BPA_CON_YEAR, df_BPA_IND_YEAR), axis=0)
            BPP = pd.concat((df_BPP_CON_YEAR, df_BPP_IND_YEAR), axis=0)
            DFC_MI = pd.concat((df_DFC_MI_CON_YEAR, df_DFC_MI_IND_YEAR), axis=0)
            DRE = pd.concat((df_DRE_CON_YEAR, df_DRE_IND_YEAR), axis=0)
            ABERTA = df_ABERTA_YEAR

            # Cria coluna mostrando que é DFP OU ITR
            BPA.insert(1, "DFP_ITR", "ITR", allow_duplicates=False)
            BPP.insert(1, "DFP_ITR", "ITR", allow_duplicates=False)
            DFC_MI.insert(1, "DFP_ITR", "ITR", allow_duplicates=False)
            DRE.insert(1, "DFP_ITR", "ITR", allow_duplicates=False)

            # Essa parte vai demorar para inserir as informações no banco.
            print(f'inserindo BPA_ITR {listaData[x]}')
            BPA.to_sql('BPA', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo BPP_ITR {listaData[x]}')
            BPP.to_sql('BPP', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo DFC_MI_ITR {listaData[x]}')
            DFC_MI.to_sql('DFC_MI', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo DRE_ITR {listaData[x]}')
            DRE.to_sql('DRE', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo CADASTRO_ITR {listaData[x]}')
            ABERTA.to_sql('CADASTRO_ITR', con=eng_connection, if_exists='append', index=True)
        except:
            print(f'Item na listadatas não encontrado no cvm (ITR) {listaData[x]}')

print('Iniciando valores no DFP')
# segunda parte do código vai preparar as DFPs para o banco de dados
if DFP:

    for x in range(len(listaData)):
        print(f'indo para o ano {listaData[x]}')
        # noinspection PyBroadException
        try:
            link = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_' + listaData[x] + '.zip'

            # Declarado cada variável que vai receber qual String com nome identico dentro do zip

            ABERTA_YEAR = 'dfp_cia_aberta_' + listaData[x] + '.csv'
            BPA_CON_YEAR = 'dfp_cia_aberta_BPA_con_' + listaData[x] + '.csv'
            BPA_IND_YEAR = 'dfp_cia_aberta_BPA_ind_' + listaData[x] + '.csv'
            BPP_CON_YEAR = 'dfp_cia_aberta_BPP_con_' + listaData[x] + '.csv'
            BPP_IND_YEAR = 'dfp_cia_aberta_BPP_ind_' + listaData[x] + '.csv'
            DFC_MI_CON_YEAR = 'dfp_cia_aberta_DFC_MI_con_' + listaData[x] + '.csv'
            DFC_MI_IND_YEAR = 'dfp_cia_aberta_DFC_MI_ind_' + listaData[x] + '.csv'
            DRE_CON_YEAR = 'dfp_cia_aberta_DRE_con_' + listaData[x] + '.csv'
            DRE_IND_YEAR = 'dfp_cia_aberta_DRE_ind_' + listaData[x] + '.csv'

            # Acessa os dados do link

            r = requests.get(link)

            # Com a biblioteca Zip é possivel visualisar os dados sem ter de baixar eles (fica na memória ram)
            # e salva essas informações na variável zf
            # https://docs.python.org/3/library/zipfile.html?highlight=zip#module-zipfile

            # IO é uma biblioteca usada para trabalhar com leitura e escrita de arquivos, mas pode ser usando para
            # valores binarios e dados crus. https://docs.python.org/3/library/io.html

            zf = zipfile.ZipFile(io.BytesIO(r.content))

            # já declarada no códico de bloco salva cada arquivo CVS que está no arquivo compactado
            # para uma nova variável

            zf_ABERTA_YEAR = zf.open(ABERTA_YEAR)
            zf_BPA_CON_YEAR = zf.open(BPA_CON_YEAR)
            zf_BPA_IND_YEAR = zf.open(BPA_IND_YEAR)
            zf_BPP_CON_YEAR = zf.open(BPP_CON_YEAR)
            zf_BPP_IND_YEAR = zf.open(BPP_IND_YEAR)
            zf_DFC_MI_CON_YEAR = zf.open(DFC_MI_CON_YEAR)
            zf_DFC_MI_IND_YEAR = zf.open(DFC_MI_IND_YEAR)
            zf_DRE_CON_YEAR = zf.open(DRE_CON_YEAR)
            zf_DRE_IND_YEAR = zf.open(DRE_IND_YEAR)

            # realizará a leitura nas linhas para futura tratativa

            l_ABERTA_YEAR = zf_ABERTA_YEAR.readlines()
            l_BPA_CON_YEAR = zf_BPA_CON_YEAR.readlines()
            l_BPA_IND_YEAR = zf_BPA_IND_YEAR.readlines()
            l_BPP_CON_YEAR = zf_BPP_CON_YEAR.readlines()
            l_BPP_IND_YEAR = zf_BPP_IND_YEAR.readlines()
            l_DFC_MI_CON_YEAR = zf_DFC_MI_CON_YEAR.readlines()
            l_DFC_MI_IND_YEAR = zf_DFC_MI_IND_YEAR.readlines()
            l_DRE_CON_YEAR = zf_DRE_CON_YEAR.readlines()
            l_DRE_IND_YEAR = zf_DRE_IND_YEAR.readlines()

            # tratativa de informações que possuem caracteres especiais como Ç ou ~ entre outros

            l_ABERTA_YEAR = [i.strip().decode('ISO-8859-1') for i in l_ABERTA_YEAR]
            l_BPA_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPA_CON_YEAR]
            l_BPA_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPA_IND_YEAR]
            l_BPP_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPP_CON_YEAR]
            l_BPP_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_BPP_IND_YEAR]
            l_DFC_MI_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DFC_MI_CON_YEAR]
            l_DFC_MI_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DFC_MI_IND_YEAR]
            l_DRE_CON_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DRE_CON_YEAR]
            l_DRE_IND_YEAR = [i.strip().decode('ISO-8859-1') for i in l_DRE_IND_YEAR]

            # Arquivo CSV normalmente separa os valores(células) por ";" é isso que estamos
            # organizando abaixo

            l_ABERTA_YEAR = [i.split(';') for i in l_ABERTA_YEAR]
            l_BPA_CON_YEAR = [i.split(';') for i in l_BPA_CON_YEAR]
            l_BPA_IND_YEAR = [i.split(';') for i in l_BPA_IND_YEAR]
            l_BPP_CON_YEAR = [i.split(';') for i in l_BPP_CON_YEAR]
            l_BPP_IND_YEAR = [i.split(';') for i in l_BPP_IND_YEAR]
            l_DFC_MI_CON_YEAR = [i.split(';') for i in l_DFC_MI_CON_YEAR]
            l_DFC_MI_IND_YEAR = [i.split(';') for i in l_DFC_MI_IND_YEAR]
            l_DRE_CON_YEAR = [i.split(';') for i in l_DRE_CON_YEAR]
            l_DRE_IND_YEAR = [i.split(';') for i in l_DRE_IND_YEAR]

            # Aqui é de fato já usando a biblioteca pandas(DataFrame) para visualisação.
            # Se apenas escrever o nome da variável é possível visualizar as informações PRONTAS
            # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html

            df_ABERTA_YEAR = pd.DataFrame(l_ABERTA_YEAR[1:], columns=l_ABERTA_YEAR[0])
            df_BPA_CON_YEAR = pd.DataFrame(l_BPA_CON_YEAR[1:], columns=l_BPA_CON_YEAR[0])
            df_BPA_IND_YEAR = pd.DataFrame(l_BPA_IND_YEAR[1:], columns=l_BPA_IND_YEAR[0])
            df_BPP_CON_YEAR = pd.DataFrame(l_BPP_CON_YEAR[1:], columns=l_BPP_CON_YEAR[0])
            df_BPP_IND_YEAR = pd.DataFrame(l_BPP_IND_YEAR[1:], columns=l_BPP_IND_YEAR[0])
            df_DFC_MI_CON_YEAR = pd.DataFrame(l_DFC_MI_CON_YEAR[1:], columns=l_DFC_MI_CON_YEAR[0])
            df_DFC_MI_IND_YEAR = pd.DataFrame(l_DFC_MI_IND_YEAR[1:], columns=l_DFC_MI_IND_YEAR[0])
            df_DRE_CON_YEAR = pd.DataFrame(l_DRE_CON_YEAR[1:], columns=l_DRE_CON_YEAR[0])
            df_DRE_IND_YEAR = pd.DataFrame(l_DRE_IND_YEAR[1:], columns=l_DRE_IND_YEAR[0])

            # Junta as informações uma abaixo da outra
            # NOVO metodo replace usando pTrus(pd)
            # https://pandas.pydata.org/docs/reference/api/pandas.concat.html

            BPA = pd.concat((df_BPA_CON_YEAR, df_BPA_IND_YEAR), axis=0)
            BPP = pd.concat((df_BPP_CON_YEAR, df_BPP_IND_YEAR), axis=0)
            DFC_MI = pd.concat((df_DFC_MI_CON_YEAR, df_DFC_MI_IND_YEAR), axis=0)
            DRE = pd.concat((df_DRE_CON_YEAR, df_DRE_IND_YEAR), axis=0)
            ABERTA = df_ABERTA_YEAR

            # Cria coluna mostrando que é DFP OU ITR
            BPA.insert(1, "DFP_ITR", "DFP", allow_duplicates=False)
            BPP.insert(1, "DFP_ITR", "DFP", allow_duplicates=False)
            DFC_MI.insert(1, "DFP_ITR", "DFP", allow_duplicates=False)
            DRE.insert(1, "DFP_ITR", "DFP", allow_duplicates=False)

            print('Essa parte vai demorar para inserir as informações no banco.')

            print(f'inserindo BPA_DFP {listaData[x]}')
            BPA.to_sql('BPA', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo BPP_DFP {listaData[x]}')
            BPP.to_sql('BPP', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo DFC_MI_DFP {listaData[x]}')
            DFC_MI.to_sql('DFC_MI', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo DRE_DFP {listaData[x]}')
            DRE.to_sql('DRE', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo CADASTRO_DFP {listaData[x]}')
            ABERTA.to_sql('CADASTRO', con=eng_connection, if_exists='append', index=True)

        except:
            print(f'Item na listadatas não encontrado no cvm (DFP) {listaData[x]}')

print(f'Fim de código')
