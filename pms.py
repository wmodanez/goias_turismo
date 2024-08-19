import os
import sys

import pandas as pd
import sidrapy
from dotenv import load_dotenv


def table_download(table_code: int, territorial_level: int, period: str, variable: list, classifications: dict,
                   ibge_territorial_code: str = 'all'):
    retorno = sidrapy.get_table(table_code=table_code, territorial_level=territorial_level,
                      ibge_territorial_code=ibge_territorial_code, period=period, variable=variable,
                      classifications=classifications)
    return retorno


def main():
    args = sys.argv[1:]
# if __name__ == '__main__':

    load_dotenv()

    # Concatenar as tabelas para um único DataFrame
    df_pms = pd.concat([
        table_download(table_code="8694", territorial_level="3", ibge_territorial_code="all", period="201501-202212",
                       variable='11623,11624,11625,11626', classifications={"11046": "56727,56728"}).rename(
            columns={'Unidade da Federação (Código)': 'Brasil e Unidade da Federação (Código)',
                     'Unidade da Federação': 'Brasil e Unidade da Federação'}),
        table_download(table_code="8694", territorial_level="3", ibge_territorial_code="all", period="202301-202406",
                       variable='11623,11624,11625,11626', classifications={"11046": "56727,56728"}).rename(
            columns={'Unidade da Federação (Código)': 'Brasil e Unidade da Federação (Código)',
                     'Unidade da Federação': 'Brasil e Unidade da Federação'}),
        table_download(table_code="8694", territorial_level="1", ibge_territorial_code="all", period="201501-202406",
                       variable='11623,11624,11625,11626', classifications={"11046": "56727,56728"}).rename(
            columns={'Brasil (Código)': 'Brasil e Unidade da Federação (Código)',
                     'Brasil': 'Brasil e Unidade da Federação'})], ignore_index=True)

    # Renomear as colunas para melhorar a legibilidade
    pms_headers = df_pms.iloc[0]
    df_pms = df_pms[1:]
    df_pms.columns = pms_headers

    df_pms['Variável'].unique()
    df_pms['Tipos de índice'].unique()

    df_pms['Variável'] = df_pms['Variável'].str.replace('mês/mês', 'mês a mês')
    df_pms['Variável'] = df_pms['Variável'].str.replace('mês/mesmo', 'mês a mesmo')
    df_pms['Variável'] = df_pms['Variável'].str.replace(' (M/M-12)', '')
    df_pms['Variável'] = df_pms['Variável'].str.replace(' (M/M-1)', '')
    df_pms['Variável'] = df_pms['Variável'].str.replace('PMS - ', '')

    # Salvar a tabela em um arquivo CSV
    df_pms.to_csv(os.getenv('PATH_FINALIZADOS') + 'pms.csv', index=False, sep=';', header='true', encoding='UTF-8')
