from sqlalchemy import create_engine  # biblioteca de conexão com SQL Server
import pandas as pd  # Biblioteca usada para tratar os dados
import zipfile  #
import io  # Melhora a flexiblidade de leitura e escrita de arquivos em conjunto do zipfile
import requests as requests  # conexção com link

# Lista dos anos fazer Download, quanto mais anos, mais demorado
listaDataFII = ["2018", "2019", "2020", "2021", "2022"]

########################################################################################################################
#  configuração abaixo serve para checar se VOCE quer rodar o script em todas AS DATAS do FII ou apenas em ALGUMAS
# "True" irá importar para o banco de dados(baseado nos anos acima), "False" não executará o código.
mensal = True
trimestral = True
anual = True
########################################################################################################################

# Conexão com Banco usando a biblioteca SQLAlchemy
# voce deve colocar o seu servidor, no meu caso é:
Serviodor = "FELIPE-LENOVO"

# A base de dados deve ser igual ao do SQL Server criada
BaseDeDados = "CVM_DW_FII"

# Caso use um perfil para acessar o servidor (recomendável)
# se não tiver um perfil (ou ignorar o tutorial que passei na documentação, pode usar o código comentado abaixo,
# e comentar o segundo driver) pode usar o do próprio do windows.
# Driver = "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=Yes"
Login = "Hedward"
Senha = "xxx"
# normalmente o driver vai ser esse de baixo, levando-se em conta que é no SQL Server
Driver = "driver=ODBC+Driver+17+for+SQL+Server"

# Biblioteca de conexão com o banco de dados
engine = create_engine(f"mssql+pyodbc://{Login}:{Senha}@{Serviodor}/{BaseDeDados}?{Driver}")
eng_connection = engine.connect()

if mensal:
    print('Iniciando valores no ITR Mensal')
    # primeira parte do código vai preparar as ITRs para o banco de dados
    for x in range(len(listaDataFII)):
        print(f'Indo para o ano {listaDataFII[x]} performatico mensal')

        # noinspection PyBroadException
        try:
            link = f'http://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_{listaDataFII[x]}.zip'

            # Declarado de forma especificar cada variável vai receber qual documento de dentro da
            # ligação(link) que possue uma compressão de ficheiros(zip)

            # As variaveis só vão receber a String por agora, essa String vai servir como referência para receb
            # arquivo do zipfile

            FII_MEN_ATIVO_PASSIVO = 'inf_mensal_fii_ativo_passivo_' + listaDataFII[x] + '.csv'
            FII_MEN_COMPLEMENTO = 'inf_mensal_fii_complemento_' + listaDataFII[x] + '.csv'
            FII_MEN_GERAL = 'inf_mensal_fii_geral_' + listaDataFII[x] + '.csv'

            # Acessa os dados do link

            r = requests.get(link)

            # Com a biblioteca Zip é possivel visualisar os dados sem ter de baixar eles
            # e salva essas informações na variável zf

            zf = zipfile.ZipFile(io.BytesIO(r.content))

            # já declarada no códico de bloco salva cada arquivo CVS que está no arquivo compactado
            # para uma nova variável
            zf_FII_MEN_ATIVO_PASSIVO = zf.open(FII_MEN_ATIVO_PASSIVO)
            zf_FII_MEN_COMPLEMENTO = zf.open(FII_MEN_COMPLEMENTO)
            zf_FII_MEN_GERAL = zf.open(FII_MEN_GERAL)

            # realizará a leitura nas linhas para futura tratativa

            l_FII_MEN_ATIVO_PASSIVO = zf_FII_MEN_ATIVO_PASSIVO.readlines()
            l_FII_MEN_COMPLEMENTO = zf_FII_MEN_COMPLEMENTO.readlines()
            l_FII_MEN_GERAL = zf_FII_MEN_GERAL.readlines()

            # tratativa de informações que possuem caracteres especiais como Ç ou ~ entre outros

            l_FII_MEN_ATIVO_PASSIVO = [i.strip().decode('ISO-8859-1') for i in l_FII_MEN_ATIVO_PASSIVO]
            l_FII_MEN_COMPLEMENTO = [i.strip().decode('ISO-8859-1') for i in l_FII_MEN_COMPLEMENTO]
            l_FII_MEN_GERAL = [i.strip().decode('ISO-8859-1') for i in l_FII_MEN_GERAL]

            # Arquivo CSV normalmente separa os valores(células) por ";" é isso que estamos
            # organizando abaixo

            l_FII_MEN_ATIVO_PASSIVO = [i.split(';') for i in l_FII_MEN_ATIVO_PASSIVO]
            l_FII_MEN_COMPLEMENTO = [i.split(';') for i in l_FII_MEN_COMPLEMENTO]
            l_FII_MEN_GERAL = [i.split(';') for i in l_FII_MEN_GERAL]

            # Aqui é de fato já usando a biblioteca pandas(DataFrame) para visualisação.
            # Se apenas escrever o nome da variável é possível visualizar as informações PRONTAS
            # print('df_FII_MEN_ATIVO_PASSIVO')

            df_FII_MEN_ATIVO_PASSIVO = pd.DataFrame(l_FII_MEN_ATIVO_PASSIVO[1:], columns=l_FII_MEN_ATIVO_PASSIVO[0])
            df_FII_MEN_COMPLEMENTO = pd.DataFrame(l_FII_MEN_COMPLEMENTO[1:], columns=l_FII_MEN_COMPLEMENTO[0])
            df_FII_MEN_GERAL = pd.DataFrame(l_FII_MEN_GERAL[1:], columns=l_FII_MEN_GERAL[0])

            # Essa parte vai demorar para inserir as informações no banco.
            print(f'inserindo FII_MEN_ATIVO_PASSIVO {listaDataFII[x]}')
            df_FII_MEN_ATIVO_PASSIVO.to_sql('mensal_fii_ativo_passivo', con=eng_connection, if_exists='append',
                                            index=True)

            print(f'inserindo FII_MEN_COMPLEMENTO {listaDataFII[x]}')
            df_FII_MEN_COMPLEMENTO.to_sql('mensal_fii_complemento', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo FII_MEN_GERAL {listaDataFII[x]}')
            df_FII_MEN_GERAL.to_sql('mensal_fii_geral', con=eng_connection, if_exists='append', index=True)

        except:
            print(f'Ultimo ano {listaDataFII[x]} não foi localizado no ZIP, '
                  f'pois ainda não tem informações anuais desse periodo')

if trimestral:

    print('Iniciando valores no ITR trimestral')
    # primeira parte do código vai preparar as ITRs para o banco de dados

    for x in range(len(listaDataFII)):
        print(f'Indo para o ano {listaDataFII[x]} performatico trimestral')

        # noinspection PyBroadException
        try:

            link = f'http://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/' \
                   f'inf_trimestral_fii_{listaDataFII[x]}.zip'

            # Declarado de forma especificar cada variável vai receber qual documento de dentro da
            # ligação(link) que possue uma compressão de ficheiros(zip)

            # As variaveis só vão receber a String por agora, essa String vai servir como referência para receb
            # arquivo do zipfile

            fii_alienacao_imovel = 'inf_trimestral_fii_alienacao_imovel_' + listaDataFII[x] + '.csv'
            fii_alienacao_terreno = 'inf_trimestral_fii_alienacao_terreno_' + listaDataFII[x] + '.csv'
            fii_aquisicao_imovel = 'inf_trimestral_fii_aquisicao_imovel_' + listaDataFII[x] + '.csv'
            fii_aquisicao_terreno = 'inf_trimestral_fii_aquisicao_terreno_' + listaDataFII[x] + '.csv'
            fii_ativo = 'inf_trimestral_fii_ativo_' + listaDataFII[x] + '.csv'
            fii_ativo_garantia_rentabilidade = 'inf_trimestral_fii_ativo_garantia_rentabilidade_' + listaDataFII[
                x] + '.csv'
            fii_complemento = 'inf_trimestral_fii_complemento_' + listaDataFII[x] + '.csv'
            fii_direito = 'inf_trimestral_fii_direito_' + listaDataFII[x] + '.csv'
            fii_geral = 'inf_trimestral_fii_geral_' + listaDataFII[x] + '.csv'
            fii_imovel = 'inf_trimestral_fii_imovel_' + listaDataFII[x] + '.csv'
            fii_imovel_desempenho = 'inf_trimestral_fii_imovel_desempenho_' + listaDataFII[x] + '.csv'
            fii_imovel_renda_acabado_contrato = 'inf_trimestral_fii_imovel_renda_acabado_contrato_' + listaDataFII[
                x] + '.csv'
            fii_imovel_renda_acabado_inquilino = 'inf_trimestral_fii_imovel_renda_acabado_inquilino_' + listaDataFII[
                x] + '.csv'
            fii_rentabilidade_efetiva = 'inf_trimestral_fii_rentabilidade_efetiva_' + listaDataFII[x] + '.csv'
            fii_resultado_contabil_financeiro = 'inf_trimestral_fii_resultado_contabil_financeiro_' + listaDataFII[
                x] + '.csv'
            fii_terreno = 'inf_trimestral_fii_terreno_' + listaDataFII[x] + '.csv'

            # Acessa os dados do link

            r = requests.get(link)

            # Com a biblioteca Zip é possivel visualisar os dados sem ter de baixar eles
            # e salva essas informações na variável zf

            zf = zipfile.ZipFile(io.BytesIO(r.content))

            # já declarada no códico de bloco salva cada arquivo CVS que está no arquivo compactado
            # para uma nova variável
            zf_fii_alienacao_imovel = zf.open(fii_alienacao_imovel)
            zf_fii_alienacao_terreno = zf.open(fii_alienacao_terreno)
            zf_fii_aquisicao_imovel = zf.open(fii_aquisicao_imovel)
            zf_fii_aquisicao_terreno = zf.open(fii_aquisicao_terreno)
            zf_fii_ativo = zf.open(fii_ativo)
            zf_fii_ativo_garantia_rentabilidade = zf.open(fii_ativo_garantia_rentabilidade)
            zf_fii_complemento = zf.open(fii_complemento)
            zf_fii_direito = zf.open(fii_direito)
            zf_fii_geral = zf.open(fii_geral)
            zf_fii_imovel = zf.open(fii_imovel)
            zf_fii_imovel_desempenho = zf.open(fii_imovel_desempenho)
            zf_fii_imovel_renda_acabado_contrato = zf.open(fii_imovel_renda_acabado_contrato)
            zf_fii_imovel_renda_acabado_inquilino = zf.open(fii_imovel_renda_acabado_inquilino)
            zf_fii_rentabilidade_efetiva = zf.open(fii_rentabilidade_efetiva)
            zf_fii_resultado_contabil_financeiro = zf.open(fii_resultado_contabil_financeiro)
            zf_fii_terreno = zf.open(fii_terreno)

            # realizará a leitura nas linhas para futura tratativa

            l_fii_alienacao_imovel = zf_fii_alienacao_imovel.readlines()
            l_fii_alienacao_terreno = zf_fii_alienacao_terreno.readlines()
            l_fii_aquisicao_imovel = zf_fii_aquisicao_imovel.readlines()
            l_fii_aquisicao_terreno = zf_fii_aquisicao_terreno.readlines()
            l_fii_ativo = zf_fii_ativo.readlines()
            l_fii_ativo_garantia_rentabilidade = zf_fii_ativo_garantia_rentabilidade.readlines()
            l_fii_complemento = zf_fii_complemento.readlines()
            l_fii_direito = zf_fii_direito.readlines()
            l_fii_geral = zf_fii_geral.readlines()
            l_fii_imovel = zf_fii_imovel.readlines()
            l_fii_imovel_desempenho = zf_fii_imovel_desempenho.readlines()
            l_fii_imovel_renda_acabado_contrato = zf_fii_imovel_renda_acabado_contrato.readlines()
            l_fii_imovel_renda_acabado_inquilino = zf_fii_imovel_renda_acabado_inquilino.readlines()
            l_fii_rentabilidade_efetiva = zf_fii_rentabilidade_efetiva.readlines()
            l_fii_resultado_contabil_financeiro = zf_fii_resultado_contabil_financeiro.readlines()
            l_fii_terreno = zf_fii_terreno.readlines()

            # tratativa de informações que possuem caracteres especiais como Ç ou ~ entre outros

            l_fii_alienacao_imovel = [i.strip().decode('ISO-8859-1') for i in l_fii_alienacao_imovel]
            l_fii_alienacao_terreno = [i.strip().decode('ISO-8859-1') for i in l_fii_alienacao_terreno]
            l_fii_aquisicao_imovel = [i.strip().decode('ISO-8859-1') for i in l_fii_aquisicao_imovel]
            l_fii_aquisicao_terreno = [i.strip().decode('ISO-8859-1') for i in l_fii_aquisicao_terreno]
            l_fii_ativo = [i.strip().decode('ISO-8859-1') for i in l_fii_ativo]
            l_fii_ativo_garantia_rentabilidade = [i.strip().decode('ISO-8859-1') for i in
                                                  l_fii_ativo_garantia_rentabilidade]
            l_fii_complemento = [i.strip().decode('ISO-8859-1') for i in l_fii_complemento]
            l_fii_direito = [i.strip().decode('ISO-8859-1') for i in l_fii_direito]
            l_fii_geral = [i.strip().decode('ISO-8859-1') for i in l_fii_geral]
            l_fii_imovel = [i.strip().decode('ISO-8859-1') for i in l_fii_imovel]
            l_fii_imovel_desempenho = [i.strip().decode('ISO-8859-1') for i in l_fii_imovel_desempenho]
            l_fii_imovel_renda_acabado_contrato = [i.strip().decode('ISO-8859-1') for i in
                                                   l_fii_imovel_renda_acabado_contrato]
            l_fii_imovel_renda_acabado_inquilino = [i.strip().decode('ISO-8859-1') for i in
                                                    l_fii_imovel_renda_acabado_inquilino]
            l_fii_rentabilidade_efetiva = [i.strip().decode('ISO-8859-1') for i in l_fii_rentabilidade_efetiva]
            l_fii_resultado_contabil_financeiro = [i.strip().decode('ISO-8859-1') for i in
                                                   l_fii_resultado_contabil_financeiro]
            l_fii_terreno = [i.strip().decode('ISO-8859-1') for i in l_fii_terreno]

            # Arquivo CSV normalmente separa os valores(células) por ";" é isso que estamos
            # organizando abaixo

            l_fii_alienacao_imovel = [i.split(';') for i in l_fii_alienacao_imovel]
            l_fii_alienacao_terreno = [i.split(';') for i in l_fii_alienacao_terreno]
            l_fii_aquisicao_imovel = [i.split(';') for i in l_fii_aquisicao_imovel]
            l_fii_aquisicao_terreno = [i.split(';') for i in l_fii_aquisicao_terreno]
            l_fii_ativo = [i.split(';') for i in l_fii_ativo]
            l_fii_ativo_garantia_rentabilidade = [i.split(';') for i in l_fii_ativo_garantia_rentabilidade]
            l_fii_complemento = [i.split(';') for i in l_fii_complemento]
            l_fii_direito = [i.split(';') for i in l_fii_direito]
            l_fii_geral = [i.split(';') for i in l_fii_geral]
            l_fii_imovel = [i.split(';') for i in l_fii_imovel]
            l_fii_imovel_desempenho = [i.split(';') for i in l_fii_imovel_desempenho]
            l_fii_imovel_renda_acabado_contrato = [i.split(';') for i in l_fii_imovel_renda_acabado_contrato]
            l_fii_imovel_renda_acabado_inquilino = [i.split(';') for i in l_fii_imovel_renda_acabado_inquilino]
            l_fii_rentabilidade_efetiva = [i.split(';') for i in l_fii_rentabilidade_efetiva]
            l_fii_resultado_contabil_financeiro = [i.split(';') for i in l_fii_resultado_contabil_financeiro]
            l_fii_terreno = [i.split(';') for i in l_fii_terreno]

            # Aqui é de fato já usando a biblioteca pandas(DataFrame) para visualisação.
            # Se apenas escrever o nome da variável é possível visualizar as informações PRONTAS
            # print('df_fii_alienacao_imovel')

            df_fii_alienacao_imovel = pd.DataFrame(l_fii_alienacao_imovel[1:], columns=l_fii_alienacao_imovel[0])
            df_fii_alienacao_terreno = pd.DataFrame(l_fii_alienacao_terreno[1:], columns=l_fii_alienacao_terreno[0])
            df_fii_aquisicao_imovel = pd.DataFrame(l_fii_aquisicao_imovel[1:], columns=l_fii_aquisicao_imovel[0])
            df_fii_aquisicao_terreno = pd.DataFrame(l_fii_aquisicao_terreno[1:], columns=l_fii_aquisicao_terreno[0])
            df_fii_ativo = pd.DataFrame(l_fii_ativo[1:], columns=l_fii_ativo[0])
            df_fii_ativo_garantia_rentabilidade = pd.DataFrame(l_fii_ativo_garantia_rentabilidade[1:],
                                                               columns=l_fii_ativo_garantia_rentabilidade[0])
            df_fii_complemento = pd.DataFrame(l_fii_complemento[1:], columns=l_fii_complemento[0])
            df_fii_direito = pd.DataFrame(l_fii_direito[1:], columns=l_fii_direito[0])
            df_fii_geral = pd.DataFrame(l_fii_geral[1:], columns=l_fii_geral[0])
            df_fii_imovel = pd.DataFrame(l_fii_imovel[1:], columns=l_fii_imovel[0])
            df_fii_imovel_desempenho = pd.DataFrame(l_fii_imovel_desempenho[1:], columns=l_fii_imovel_desempenho[0])
            df_fii_imovel_renda_acabado_contrato = pd.DataFrame(l_fii_imovel_renda_acabado_contrato[1:],
                                                                columns=l_fii_imovel_renda_acabado_contrato[0])
            df_fii_imovel_renda_acabado_inquilino = pd.DataFrame(l_fii_imovel_renda_acabado_inquilino[1:],
                                                                 columns=l_fii_imovel_renda_acabado_inquilino[0])
            df_fii_rentabilidade_efetiva = pd.DataFrame(l_fii_rentabilidade_efetiva[1:],
                                                        columns=l_fii_rentabilidade_efetiva[0])
            df_fii_resultado_contabil_financeiro = pd.DataFrame(l_fii_resultado_contabil_financeiro[1:],
                                                                columns=l_fii_resultado_contabil_financeiro[0])
            df_fii_terreno = pd.DataFrame(l_fii_terreno[1:], columns=l_fii_terreno[0])

            # Essa parte vai demorar para inserir as informações no banco.
            print(f'inserindo fii_alienacao_imovel {listaDataFII[x]}')
            df_fii_alienacao_imovel.to_sql('trimestral_fii_alienacao_imovel', con=eng_connection, if_exists='append',
                                           index=True)

            print(f'inserindo fii_alienacao_terreno {listaDataFII[x]}')
            df_fii_alienacao_terreno.to_sql('trimestral_fii_alienacao_terreno', con=eng_connection, if_exists='append',
                                            index=True)

            print(f'inserindo fii_aquisicao_imovel {listaDataFII[x]}')
            df_fii_aquisicao_imovel.to_sql('trimestral_fii_aquisicao_imovel', con=eng_connection, if_exists='append',
                                           index=True)

            print(f'inserindo fii_aquisicao_terreno {listaDataFII[x]}')
            df_fii_aquisicao_terreno.to_sql('trimestral_fii_aquisicao_terreno', con=eng_connection, if_exists='append',
                                            index=True)

            print(f'inserindo fii_ativo {listaDataFII[x]}')
            df_fii_ativo.to_sql('trimestral_fii_ativo', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo fii_ativo_garantia_rentabilidade {listaDataFII[x]}')
            df_fii_ativo_garantia_rentabilidade.to_sql('trimestral_fii_ativo_garantia_rentabilidade',
                                                       con=eng_connection, if_exists='append', index=True)

            print(f'inserindo fii_complemento {listaDataFII[x]}')
            df_fii_complemento.to_sql('trimestral_fii_complemento', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo fii_direito {listaDataFII[x]}')
            df_fii_direito.to_sql('trimestral_fii_direito', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo fii_geral {listaDataFII[x]}')
            df_fii_geral.to_sql('trimestral_fii_geral', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo fii_imovel {listaDataFII[x]}')
            df_fii_imovel.to_sql('trimestral_fii_imovel', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo fii_imovel_desempenho {listaDataFII[x]}')
            df_fii_imovel_desempenho.to_sql('trimestral_fii_imovel_desempenho', con=eng_connection, if_exists='append',
                                            index=True)

            print(f'inserindo fii_imovel_renda_acabado_contrato {listaDataFII[x]}')
            df_fii_imovel_renda_acabado_contrato.to_sql('trimestral_fii_imovel_renda_acabado_contrato',
                                                        con=eng_connection,
                                                        if_exists='append', index=True)

            print(f'inserindo fii_imovel_renda_acabado_inquilino {listaDataFII[x]}')
            df_fii_imovel_renda_acabado_inquilino.to_sql('trimestral_fii_imovel_renda_acabado_inquilino',
                                                         con=eng_connection,
                                                         if_exists='append', index=True)

            print(f'inserindo fii_rentabilidade_efetiva {listaDataFII[x]}')
            df_fii_rentabilidade_efetiva.to_sql('trimestral_fii_rentabilidade_efetiva', con=eng_connection,
                                                if_exists='append',
                                                index=True)

            print(f'inserindo fii_resultado_contabil_financeiro {listaDataFII[x]}')
            df_fii_resultado_contabil_financeiro.to_sql('trimestral_fii_resultado_contabil_financeiro',
                                                        con=eng_connection,
                                                        if_exists='append', index=True)

            print(f'inserindo fii_terreno {listaDataFII[x]}')
            df_fii_terreno.to_sql('trimestral_fii_terreno', con=eng_connection, if_exists='append', index=True)
        except:
            print(f'Ultimo ano {listaDataFII[x]} não foi localizado no ZIP,'
                  f'pois ainda não tem informações trimestrais desse periodo')

if anual:

    print('Iniciando valores no ITR Anual')
    # primeira parte do código vai preparar as ITRs para o banco de dados
    for x in range(len(listaDataFII)):
        print(f'Indo para o ano {listaDataFII[x]} performatico anual')

        # noinspection PyBroadException
        try:

            link = f'http://dados.cvm.gov.br/dados/FII/DOC/INF_ANUAL/DADOS/inf_anual_fii_{listaDataFII[x]}.zip'

            # Declarado de forma especificar cada variável vai receber qual documento de dentro da
            # ligação(link) que possue uma compressão de ficheiros(zip)

            # As variaveis só vão receber a String por agora, essa String vai servir como referência para receb arquivo
            # do zipfile

            anual_fii_ativo_adquirido = 'inf_anual_fii_ativo_adquirido_' + listaDataFII[x] + '.csv'
            anual_fii_ativo_transacao = 'inf_anual_fii_ativo_transacao_' + listaDataFII[x] + '.csv'
            anual_fii_ativo_valor_contabil = 'inf_anual_fii_ativo_valor_contabil_' + listaDataFII[x] + '.csv'
            anual_fii_complemento = 'inf_anual_fii_complemento_' + listaDataFII[x] + '.csv'
            anual_fii_diretor_responsavel = 'inf_anual_fii_diretor_responsavel_' + listaDataFII[x] + '.csv'
            anual_fii_distribuicao_cotistas = 'inf_anual_fii_distribuicao_cotistas_' + listaDataFII[x] + '.csv'
            anual_fii_experiencia_profissional = 'inf_anual_fii_experiencia_profissional_' + listaDataFII[x] + '.csv'
            anual_fii_geral = 'inf_anual_fii_geral_' + listaDataFII[x] + '.csv'
            anual_fii_prestador_servico = 'inf_anual_fii_prestador_servico_' + listaDataFII[x] + '.csv'
            anual_fii_processo = 'inf_anual_fii_processo_' + listaDataFII[x] + '.csv'
            anual_fii_processo_semelhante = 'inf_anual_fii_processo_semelhante_' + listaDataFII[x] + '.csv'
            anual_fii_representante_cotista = 'inf_anual_fii_representante_cotista_' + listaDataFII[x] + '.csv'
            anual_fii_representante_cotista_fundo = 'inf_anual_fii_representante_cotista_fundo_' + listaDataFII[
                x] + '.csv'

            # Acessa os dados do link

            r = requests.get(link)

            # Com a biblioteca Zip é possivel visualisar os dados sem ter de baixar eles
            # e salva essas informações na variável zf

            zf = zipfile.ZipFile(io.BytesIO(r.content))

            # já declarada no códico de bloco salva cada arquivo CVS que está no arquivo compactado
            # para uma nova variável
            zf_anual_fii_ativo_adquirido = zf.open(anual_fii_ativo_adquirido)
            zf_anual_fii_ativo_transacao = zf.open(anual_fii_ativo_transacao)
            zf_anual_fii_ativo_valor_contabil = zf.open(anual_fii_ativo_valor_contabil)
            zf_anual_fii_complemento = zf.open(anual_fii_complemento)
            zf_anual_fii_diretor_responsavel = zf.open(anual_fii_diretor_responsavel)
            zf_anual_fii_distribuicao_cotistas = zf.open(anual_fii_distribuicao_cotistas)
            zf_anual_fii_experiencia_profissional = zf.open(anual_fii_experiencia_profissional)
            zf_anual_fii_geral = zf.open(anual_fii_geral)
            zf_anual_fii_prestador_servico = zf.open(anual_fii_prestador_servico)
            zf_anual_fii_processo = zf.open(anual_fii_processo)
            zf_anual_fii_processo_semelhante = zf.open(anual_fii_processo_semelhante)
            zf_anual_fii_representante_cotista = zf.open(anual_fii_representante_cotista)
            zf_anual_fii_representante_cotista_fundo = zf.open(anual_fii_representante_cotista_fundo)

            # realizará a leitura nas linhas para futura tratativa
            l_anual_fii_ativo_adquirido = zf_anual_fii_ativo_adquirido.readlines()
            l_anual_fii_ativo_transacao = zf_anual_fii_ativo_transacao.readlines()
            l_anual_fii_ativo_valor_contabil = zf_anual_fii_ativo_valor_contabil.readlines()
            l_anual_fii_complemento = zf_anual_fii_complemento.readlines()
            l_anual_fii_diretor_responsavel = zf_anual_fii_diretor_responsavel.readlines()
            l_anual_fii_distribuicao_cotistas = zf_anual_fii_distribuicao_cotistas.readlines()
            l_anual_fii_experiencia_profissional = zf_anual_fii_experiencia_profissional.readlines()
            l_anual_fii_geral = zf_anual_fii_geral.readlines()
            l_anual_fii_prestador_servico = zf_anual_fii_prestador_servico.readlines()
            l_anual_fii_processo = zf_anual_fii_processo.readlines()
            l_anual_fii_processo_semelhante = zf_anual_fii_processo_semelhante.readlines()
            l_anual_fii_representante_cotista = zf_anual_fii_representante_cotista.readlines()
            l_anual_fii_representante_cotista_fundo = zf_anual_fii_representante_cotista_fundo.readlines()

            # tratativa de informações que possuem caracteres especiais como Ç ou ~ entre outros

            l_anual_fii_ativo_adquirido = [i.strip().decode('ISO-8859-1') for i in l_anual_fii_ativo_adquirido]
            l_anual_fii_ativo_transacao = [i.strip().decode('ISO-8859-1') for i in l_anual_fii_ativo_transacao]
            l_anual_fii_ativo_valor_contabil = [i.strip().decode('ISO-8859-1') for i in
                                                l_anual_fii_ativo_valor_contabil]
            l_anual_fii_complemento = [i.strip().decode('ISO-8859-1') for i in l_anual_fii_complemento]
            l_anual_fii_diretor_responsavel = [i.strip().decode('ISO-8859-1') for i in l_anual_fii_diretor_responsavel]
            l_anual_fii_distribuicao_cotistas = [i.strip().decode('ISO-8859-1') for i in
                                                 l_anual_fii_distribuicao_cotistas]
            l_anual_fii_experiencia_profissional = [i.strip().decode('ISO-8859-1') for i in
                                                    l_anual_fii_experiencia_profissional]
            l_anual_fii_geral = [i.strip().decode('ISO-8859-1') for i in l_anual_fii_geral]
            l_anual_fii_prestador_servico = [i.strip().decode('ISO-8859-1') for i in l_anual_fii_prestador_servico]
            l_anual_fii_processo = [i.strip().decode('ISO-8859-1') for i in l_anual_fii_processo]
            l_anual_fii_processo_semelhante = [i.strip().decode('ISO-8859-1') for i in
                                               l_anual_fii_processo_semelhante]
            l_anual_fii_representante_cotista = [i.strip().decode('ISO-8859-1') for i in
                                                 l_anual_fii_representante_cotista]
            l_anual_fii_representante_cotista_fundo = [i.strip().decode('ISO-8859-1') for i in
                                                       l_anual_fii_representante_cotista_fundo]

            # Arquivo CSV normalmente separa os valores(células) por ";" é isso que estamos
            # organizando abaixo

            l_anual_fii_ativo_adquirido = [i.split(';') for i in l_anual_fii_ativo_adquirido]
            l_anual_fii_ativo_transacao = [i.split(';') for i in l_anual_fii_ativo_transacao]
            l_anual_fii_ativo_valor_contabil = [i.split(';') for i in l_anual_fii_ativo_valor_contabil]
            l_anual_fii_complemento = [i.split(';') for i in l_anual_fii_complemento]
            l_anual_fii_diretor_responsavel = [i.split(';') for i in l_anual_fii_diretor_responsavel]
            l_anual_fii_distribuicao_cotistas = [i.split(';') for i in l_anual_fii_distribuicao_cotistas]
            l_anual_fii_experiencia_profissional = [i.split(';') for i in l_anual_fii_experiencia_profissional]
            l_anual_fii_geral = [i.split(';') for i in l_anual_fii_geral]
            l_anual_fii_prestador_servico = [i.split(';') for i in l_anual_fii_prestador_servico]
            l_anual_fii_processo = [i.split(';') for i in l_anual_fii_processo]
            l_anual_fii_processo_semelhante = [i.split(';') for i in l_anual_fii_processo_semelhante]
            l_anual_fii_representante_cotista = [i.split(';') for i in l_anual_fii_representante_cotista]
            l_anual_fii_representante_cotista_fundo = [i.split(';') for i in l_anual_fii_representante_cotista_fundo]

            # Aqui é de fato já usando a biblioteca pandas(DataFrame) para visualisação.
            # Se apenas escrever o nome da variável é possível visualizar as informações PRONTAS
            # print('df_anual_fii_ativo_adquirido')

            df_anual_fii_ativo_adquirido = pd.DataFrame(l_anual_fii_ativo_adquirido[1:],
                                                        columns=l_anual_fii_ativo_adquirido[0])
            df_anual_fii_ativo_transacao = pd.DataFrame(l_anual_fii_ativo_transacao[1:],
                                                        columns=l_anual_fii_ativo_transacao[0])
            df_anual_fii_ativo_valor_contabil = pd.DataFrame(l_anual_fii_ativo_valor_contabil[1:],
                                                             columns=l_anual_fii_ativo_valor_contabil[0])
            df_anual_fii_complemento = pd.DataFrame(l_anual_fii_complemento[1:], columns=l_anual_fii_complemento[0])
            df_anual_fii_diretor_responsavel = pd.DataFrame(l_anual_fii_diretor_responsavel[1:],
                                                            columns=l_anual_fii_diretor_responsavel[0])
            df_anual_fii_distribuicao_cotistas = pd.DataFrame(l_anual_fii_distribuicao_cotistas[1:],
                                                              columns=l_anual_fii_distribuicao_cotistas[0])
            df_anual_fii_experiencia_profissional = pd.DataFrame(l_anual_fii_experiencia_profissional[1:],
                                                                 columns=l_anual_fii_experiencia_profissional[0])
            df_anual_fii_geral = pd.DataFrame(l_anual_fii_geral[1:], columns=l_anual_fii_geral[0])
            df_anual_fii_prestador_servico = pd.DataFrame(l_anual_fii_prestador_servico[1:],
                                                          columns=l_anual_fii_prestador_servico[0])
            df_anual_fii_processo = pd.DataFrame(l_anual_fii_processo[1:], columns=l_anual_fii_processo[0])
            df_anual_fii_processo_semelhante = pd.DataFrame(l_anual_fii_processo_semelhante[1:],
                                                            columns=l_anual_fii_processo_semelhante[0])
            df_anual_fii_representante_cotista = pd.DataFrame(l_anual_fii_representante_cotista[1:],
                                                              columns=l_anual_fii_representante_cotista[0])
            df_anual_fii_representante_cotista_fundo = pd.DataFrame(l_anual_fii_representante_cotista_fundo[1:],
                                                                    columns=l_anual_fii_representante_cotista_fundo[0])

            # Essa parte vai demorar para inserir as informações no banco.

            print(f'inserindo anual_fii_ativo_adquirido {listaDataFII[x]}')
            df_anual_fii_ativo_adquirido.to_sql('anual_fii_ativo_adquirido', con=eng_connection, if_exists='append',
                                                index=True)

            print(f'inserindo anual_fii_ativo_transacao {listaDataFII[x]}')
            df_anual_fii_ativo_transacao.to_sql('anual_fii_ativo_transacao', con=eng_connection, if_exists='append',
                                                index=True)

            print(f'inserindo anual_fii_ativo_valor_contabil {listaDataFII[x]}')
            df_anual_fii_ativo_valor_contabil.to_sql('anual_fii_ativo_valor_contabil', con=eng_connection,
                                                     if_exists='append', index=True)

            print(f'inserindo anual_fii_complemento {listaDataFII[x]}')
            df_anual_fii_complemento.to_sql('anual_fii_complemento', con=eng_connection, if_exists='append',
                                            index=True)

            print(f'inserindo anual_fii_diretor_responsavel {listaDataFII[x]}')
            df_anual_fii_diretor_responsavel.to_sql('anual_fii_diretor_responsavel', con=eng_connection,
                                                    if_exists='append',
                                                    index=True)

            print(f'inserindo anual_fii_distribuicao_cotistas {listaDataFII[x]}')
            df_anual_fii_distribuicao_cotistas.to_sql('anual_fii_distribuicao_cotistas', con=eng_connection,
                                                      if_exists='append', index=True)

            print(f'inserindo anual_fii_experiencia_profissional {listaDataFII[x]}')
            df_anual_fii_experiencia_profissional.to_sql('anual_fii_experiencia_profissional', con=eng_connection,
                                                         if_exists='append', index=True)

            print(f'inserindo anual_fii_geral {listaDataFII[x]}')
            df_anual_fii_geral.to_sql('anual_fii_geral', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo anual_fii_prestador_servico {listaDataFII[x]}')
            df_anual_fii_prestador_servico.to_sql('anual_fii_prestador_servico', con=eng_connection,
                                                  if_exists='append', index=True)

            print(f'inserindo anual_fii_processo {listaDataFII[x]}')
            df_anual_fii_processo.to_sql('anual_fii_processo', con=eng_connection, if_exists='append', index=True)

            print(f'inserindo anual_fii_processo_semelhante {listaDataFII[x]}')
            df_anual_fii_processo_semelhante.to_sql('anual_fii_processo_semelhante', con=eng_connection,
                                                    if_exists='append', index=True)

            print(f'inserindo anual_fii_representante_cotista {listaDataFII[x]}')
            df_anual_fii_representante_cotista.to_sql('anual_fii_representante_cotista', con=eng_connection,
                                                      if_exists='append', index=True)

            print(f'inserindo anual_fii_representante_cotista_fundo {listaDataFII[x]}')
            df_anual_fii_representante_cotista_fundo.to_sql('anual_fii_representante_cotista_fundo', con=eng_connection,
                                                            if_exists='append', index=True)
        except:
            print(f'Ultimo ano {listaDataFII[x]} não foi localizado no ZIP,'
                  f'pois ainda não tem informações anuais desse periodo')

print(f'Fim de código')
