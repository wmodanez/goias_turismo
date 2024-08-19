import os
import sys

import pandas as pd
from dotenv import load_dotenv


def main():
    args = sys.argv[1:]
# if __name__ == '__main__':

    load_dotenv()

    path_ibeu = os.getenv('PATH_ORIGINAIS') + 'ibeu/'

    df_ibeu = pd.read_csv(path_ibeu + 'ibeu_go.csv', sep=';', encoding='ISO-8859-1')

    df_regioes_turismo = pd.read_csv(os.getenv('PATH_AUXILIARES') + 'dRegioesTuristicas.csv', sep=";",
                                     encoding='ISO-8859-1')

    df_regioes_turismo = df_regioes_turismo[['CODIBGE', 'Regiao_Turistica']]

    df_regioes_turismo = pd.merge(df_ibeu, df_regioes_turismo, how='left', left_on=['CODIBGE'], right_on=['CODIBGE'])

    df_regioes_turismo = df_regioes_turismo[
        ['Nome_Mun', 'Variável', 'Valor', 'Ano', 'CODIBGE', 'Categoria', 'Regiao_Turistica']]

    df_regioes_turismo.to_csv(os.getenv('PATH_FINALIZADOS') + 'ibeu_go.csv', index=False, sep=';', header='true',
                              encoding='UTF-8')
