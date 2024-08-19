import os
import re
import shutil
import sys
import warnings
import zipfile
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv



def file_zip_extractor(compressFileList: list, folderDest: str):
    for arq in compressFileList:
        print('Unpacking ' + arq)
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(folderDest)
        print(f'File unpacked to {folderDest}')


def main():
    args = sys.argv[1:]
# if __name__ == '__main__':

    load_dotenv()

    # Extrai os arquivos do zip do Cadastur
    path_cadastur = os.getenv('PATH_ORIGINAIS') + 'cadastur/'
    file_zip_extractor([os.getenv('PATH_ORIGINAIS') + os.getenv('FILE_CADASTUR')], path_cadastur)

    # Lista todos os arquivos xlsx no diretório original
    xlsx_files = os.listdir(path_cadastur)

    df_cadastur = pd.DataFrame()

    # Converte todos os xlsx em csv
    for file in os.listdir(path_cadastur):
        file = path_cadastur + file
        if not file.replace('xlsx', 'csv').startswith('~') and file.endswith('.xlsx'):
            if os.path.isfile(file.replace('xlsx', 'csv')):
                continue
            print(f'Convertendo {file} em {file.replace('xlsx', 'csv')}')
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=UserWarning, module=re.escape('openpyxl.styles.stylesheet'))
                df_temp = pd.read_excel(file, engine='openpyxl')
            df_temp.to_csv(file.replace('xlsx', 'csv'), sep=';', index=None, header=True)

    # Lê os csv e cria um dataframe com todos os dados
    for file in os.listdir(path_cadastur):
        file = path_cadastur + file
        if file.endswith('.csv'):
            if file == 'GUIA DE TURISMO - PESSOA FÍSICA.csv':
                df_auxiliar = pd.read_csv(file, sep=';', encoding='UTF-8')
                df_auxiliar['Nome Fantasia'] = ''
                df_auxiliar['Endereço Completo Comercial'] = ''
                df_auxiliar['Telefone Comercial'] = ''
                df_auxiliar['E-mail Comercial'] = ''
                df_auxiliar['Website'] = ''
                df_auxiliar['Nome'] = file
                df_cadastur = pd.concat([df_cadastur, df_auxiliar], ignore_index=True)

            else:
                df_auxiliar = pd.read_csv(file, sep=';', encoding='UTF-8')
                df_auxiliar['Nome'] = file
                df_cadastur = pd.concat([df_cadastur, df_auxiliar], ignore_index=True)

    df_cadastur['Nome'] = df_cadastur['Nome'].str.title().replace('.Csv', '')
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.title()
    df_cadastur['Endereço Completo Comercial'] = df_cadastur['Endereço Completo Comercial'].str.title()
    df_cadastur['E-mail Comercial'] = df_cadastur['E-mail Comercial'].str.lower()

    df_cadastur.dropna(subset=['Município'], inplace=True)

    df_cadastur['Município'] = df_cadastur['Município'].str.replace('Bom Jesus', 'Bom Jesus de Goiás')
    df_cadastur['Município'] = df_cadastur['Município'].str.replace('São João D\'Aliança', 'São João d\'Aliança')
    df_cadastur['Município'] = df_cadastur['Município'].str.replace('Anhangüera', 'Anhanguera')
    df_cadastur['Município'] = df_cadastur['Município'].str.replace('São Luiz do Norte', 'São Luíz do Norte')

    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace(r'\s[\d]+', '', regex=True)
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace('E', 'e')
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace('Na', 'na')
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace('De', 'de')
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace('Dos', 'dos')
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace('Em', 'em')
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace('Da', 'da')
    df_cadastur['Nome Fantasia'] = df_cadastur['Nome Fantasia'].str.replace('Do', 'do')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('De', 'de')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('Ao', 'ao')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('E', 'e')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('À', 'a')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('Ou', 'ou')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('Ao', 'au')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('Em', 'em')
    df_cadastur['Nome'] = df_cadastur['Nome'].str.replace('Mei', 'MEI')

    df_cadastur['Data da atualização'] = datetime.today().strftime('%Y/%m/%d')
    df_cadastur['Valor'] = 1

    df_auxiliar = pd.read_csv(os.getenv('path_auxiliares') + 'dRegioesTuristicas.csv', sep=';', encoding='ISO-8859-1')
    df_auxiliar = df_auxiliar[['Nome_Mun', 'Regiao_Turistica']]

    # Fazer o merge do DataFrame principal com o DataFrame auxiliar, onde a coluna 'Município' é a chave comum.
    df_cadastur = pd.merge(df_cadastur, df_auxiliar, how='left', left_on=['Município'], right_on=['Nome_Mun'])

    # Substituir os valores NaN por 0 na coluna 'Nome_Mun' e salvar o novo DataFrame
    df_cadastur.fillna({'Nome_Mun': 0}, inplace=True)

    # Filtrar as linhas desejada
    df_cadastur = df_cadastur.loc[df_cadastur['Nome_Mun'] != 0]

    # Ordenar as colunas do DataFrame final
    df_cadastur = df_cadastur[['Nome Fantasia', 'Situação Cadastral', 'Município', 'Endereço Completo Comercial',
                               'Telefone Comercial', 'E-mail Comercial', 'Website', 'Nome', 'Data da atualização',
                               'Valor', 'Regiao_Turistica']]

    # Apagar os diretórios temporários criados
    shutil.rmtree(path_cadastur)

    # Salvar o DataFrame final como um CSV na pasta Cadastur do BI Turismo Observatório.
    df_cadastur.to_csv(os.getenv('PATH_FINALIZADOS') + 'cadastur.csv', index=False, sep=';', header='true',
                       encoding='UTF-8')
