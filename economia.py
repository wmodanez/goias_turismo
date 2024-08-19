import os
import re
import sys
import warnings
import zipfile

import pandas as pd
from dotenv import load_dotenv



def file_zip_extractor(compressFileList: list, folderDest: str):
    for arq in compressFileList:
        print('Unpacking ' + arq)
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(folderDest)
        print(f'File unpacked to {folderDest}')


def cnae_sheet(df_cnae: pd.DataFrame) -> pd.DataFrame:
    # O cabecalho neste dataframe deveria ser a primeira linha
    df_cnae.loc[0] = list(df_cnae.columns)

    # Renomear as colunas do dataframe de CNAE
    df_cnae.columns = df_cnae.loc[1]

    # Criar uma coluna com numero de caracteres
    df_cnae['tamanho_subclasse'] = df_cnae['Subclasse Atual - Código'].apply(lambda x: len(str(x)))

    # Criar uma nova coluna com numero de caracteres igual aos 5 primeiros digitos
    df_cnae.loc[df_cnae['tamanho_subclasse'] == 6, 'Data'] = df_cnae['Subclasse Atual - Código']

    # Preencher pra baixo a coluna data
    with pd.option_context('future.no_silent_downcasting', True):
        df_cnae.loc[:, 'Data'] = df_cnae.loc[:, 'Data'].ffill().infer_objects()

    # Filtrar as linhas desejadas
    df_cnae = df_cnae.loc[df_cnae['tamanho_subclasse'] != 6]

    # Remover linhas vazias
    df_cnae.dropna(subset=['Subclasse Atual - Descrição'], inplace=True)
    df_cnae.fillna({'Subclasse Atual - Descrição': 0}, inplace=True)

    # Filtrar as linhas desejadas
    df_cnae = df_cnae.loc[df_cnae['Subclasse Atual - Código'] != 'Subclasse Atual - Código']

    # Ordenar as colunas do DataFrame final
    df_cnae = df_cnae.loc[df_cnae['Subclasse Atual - Descrição'] != 'Soma:']

    # Redefinir o Index
    df_cnae.reset_index(inplace=True, drop=True)

    # CNAEs do Observatório do turismo
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=UserWarning, module=re.escape('openpyxl.styles.stylesheet'))
        df_cnae_turismo = pd.read_excel(os.getenv('PATH_AUXILIARES') + 'CNAE20_Subclasses_EstruturaDetalhada.xlsx',
                                        skiprows=0, engine='openpyxl')
    df_cnae_turismo['Subclasse'].nunique()

    df_denominacao = df_cnae_turismo.copy()
    df_denominacao.fillna(0, inplace=True)
    df_denominacao['Denominação'] = df_denominacao['Denominação'].str.capitalize()

    df_secao = df_denominacao[df_denominacao['Seção'] != 0]
    df_secao = df_secao[['Seção', 'Denominação']]
    df_secao.rename(columns={'Denominação': 'Denominação_sessao'}, inplace=True)

    df_divisao = df_denominacao[df_denominacao['Divisão'] != 0]
    df_divisao = df_divisao[['Divisão', 'Denominação']]
    df_divisao.rename(columns={'Denominação': 'Denominação_divisao'}, inplace=True)

    df_grupo = df_denominacao[df_denominacao['Grupo'] != 0]
    df_grupo = df_grupo[['Grupo', 'Denominação']]
    df_grupo.rename(columns={'Denominação': 'Denominação_grupo'}, inplace=True)

    df_classe = df_denominacao[df_denominacao['Classe'] != 0]
    df_classe = df_classe[['Classe', 'Denominação']]
    df_classe.rename(columns={'Denominação': 'Denominação_classe'}, inplace=True)

    with pd.option_context('future.no_silent_downcasting', True):
        df_cnae_turismo = df_cnae_turismo.ffill(axis=0)
    df_cnae_turismo.fillna(0, inplace=True)

    df_cnae_turismo['Denominação'] = df_cnae_turismo['Denominação'].str.capitalize()
    df_cnae_turismo = df_cnae_turismo[df_cnae_turismo['Subclasse'] != 0]
    df_cnae_turismo = df_cnae_turismo.drop_duplicates(subset=['Subclasse'])
    df_cnae_turismo['Subclasse'] = df_cnae_turismo['Subclasse'].apply(lambda x: str(x).replace('-', ''))
    df_cnae_turismo['Subclasse'] = df_cnae_turismo['Subclasse'].apply(lambda x: str(x).replace('/', ''))

    df_cnae_turismo = pd.merge(df_cnae_turismo, df_secao, on='Seção', how='left')
    df_cnae_turismo = pd.merge(df_cnae_turismo, df_divisao, on='Divisão', how='left')
    df_cnae_turismo = pd.merge(df_cnae_turismo, df_grupo, on='Grupo', how='left')
    df_cnae_turismo = pd.merge(df_cnae_turismo, df_classe, on='Classe', how='left')

    df_cnae_turismo = df_cnae_turismo[
        ['Seção', 'Denominação_sessao', 'Divisão', 'Denominação_divisao', 'Grupo', 'Denominação_grupo', 'Classe',
         'Denominação_classe', 'Subclasse', 'Denominação']]

    df_cnae = pd.merge(df_cnae, df_cnae_turismo, how='left', left_on=['Subclasse Atual - Código'],
                       right_on=['Subclasse'])

    df_cnae = df_cnae[
        ['Subclasse Atual - Código', 'Subclasse Atual - Descrição', 'Valor Total', 'Qtd Contribuintes', 'Data',
         'Denominação_sessao']]

    # Renomear as colunas do tabela atividade
    df_cnae.columns = ['Cód Subclasse Atual', 'Subclasse Atual', 'Valor Total', 'Qtd Contribuintes', 'Data',
                       'Atividade']

    df_cnae.to_csv(os.getenv('PATH_FINALIZADOS') + 'cnae.csv', index=False, sep=';', header='true', encoding='UTF-8')
    return df_cnae


def icms_sheet(df_icms: pd.DataFrame) -> pd.DataFrame:
    df_icms.rename(columns={'Unnamed: 0': "Município"}, inplace=True)
    df_icms = pd.melt(df_icms, id_vars=["Município"], var_name='Ano-Mês', value_name='Valor')

    df_icms['Município'] = df_icms['Município'].str.replace("BOM JESUS", "BOM JESUS DE GOIAS")

    df_regioes_turismo = pd.read_csv(os.getenv('path_auxiliares') + 'dRegioesTuristicas.csv', sep=";",
                                     encoding='ISO-8859-1')

    df_regioes_turismo['MUN'] = df_regioes_turismo['Nome_Mun'].str.upper()
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Á", "A")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Â", "A")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Ã", "A")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("É", "E")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Ê", "E")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Í", "I")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Ó", "O")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Ô", "O")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Õ", "O")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Ú", "U")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Ç", "C")
    df_regioes_turismo['MUN'] = df_regioes_turismo['MUN'].str.replace("Ç", "C")

    # Fazer o merge dos dois dataframe
    df_icms = pd.merge(df_icms, df_regioes_turismo, how='left', left_on=['Município'], right_on=['MUN'])
    df_icms = df_icms.drop(columns=['MUN'])

    df_icms = df_icms.loc[df_icms['Ano-Mês'] != 'Soma:']
    df_icms = df_icms.loc[df_icms['Município'] != 'Soma:']
    df_icms = df_icms.loc[df_icms['Município'] != 'DISTRITO FEDERAL']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DA BAHIA']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DA PARAIBA']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DE ALAGOAS']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DE MINAS GERAIS']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DE PERNAMBUCO']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DE SAO PAULO']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DE SERGIPE']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO CEARA']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO ESPIRITO SANTO']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO MATO GROSSO']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO MATO GROSSO DO SUL']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO PARANA']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO PIAUI']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO RIO DE JANEIRO']
    df_icms = df_icms.loc[df_icms['Município'] != 'ESTADO DO RIO GRANDE DO NORTE']

    # Salvar o resultado
    df_icms.to_csv(os.getenv('PATH_FINALIZADOS') + 'icms.csv', index=False, sep=';', header=True, encoding='UTF-8')


def receita_sheet(df_receita: pd.DataFrame) -> pd.DataFrame:
    df_receita.columns.values[0] = "Imposto"

    df_receita = df_receita.set_index('Imposto').T
    df_receita = df_receita.drop('Soma:', axis=1)
    df_receita.to_csv(os.getenv('PATH_FINALIZADOS') + 'imposto.csv', sep=';')
    return df_receita


def main():
    args = sys.argv[1:]
# if __name__ == '__main__':

    load_dotenv()

    path_economia = os.getenv('PATH_ORIGINAIS') + 'economia/'
    with pd.option_context('future.no_silent_downcasting', True):
        xl = pd.ExcelFile(path_economia + os.listdir(path_economia)[0], engine='openpyxl')

    cnae_sheet(xl.parse('CNAE'))

    icms_sheet(xl.parse('Município'))

    receita_sheet(xl.parse('Receita'))
