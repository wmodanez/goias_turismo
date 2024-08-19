import os

import pandas as pd
from datetime import datetime

# os.chdir('Z:/GEDE/Desenvolvimento/Projetos Antigos/PaineisGoiasIndicadores/BI_Turismo_Observatorio/Novo_CAGED')
os.chdir('D:/CAGED')
caminho_original = os.getcwd()

tabFOR = pd.DataFrame()
tabEXC = pd.DataFrame()
tabMOV = pd.DataFrame()

columns = ['competênciamov', 'uf', 'município', 'subclasse', 'saldomovimentação',
           'categoria', 'cbo2002ocupação', 'graudeinstrução', 'idade', 'horascontratuais', 'raçacor',
           'sexo', 'tipoestabelecimento', 'tipomovimentação', 'tipodedeficiência', 'indtrabintermitente',
           'indtrabparcial', 'salário', 'indtrabparcial', 'indicadoraprendiz', 'tamestabjan',
           'origemdainformação', 'indicadordeforadoprazo']

for root, dirs, files in os.walk(caminho_original):
    for file in files:
        path = os.path.join(root, file)
        if ".txt" in file:
            if "FOR" in file:  # definir a extensão do aqruivo
                print(path)
                tab = pd.read_table(path, sep=";", encoding='utf-8', usecols=columns)
                # tab = tab[tab['uf'] == 52]
                tab["Tipo"] = 'FOR'
                tabFOR = pd.concat([tabFOR, tab], ignore_index=True)

print(tabFOR)

for root, dirs, files in os.walk(caminho_original):
    for file in files:
        path = os.path.join(root, file)
        if ".txt" in file:
            if "EXC" in file:  # definir a extensão do aqruivo
                print(path)
                tab = pd.read_table(path, sep=";", encoding='utf-8', usecols=columns)
                # tab = tab[tab['uf'] == 52]
                tab["Tipo"] = 'EXC'
                tabEXC = pd.concat([tabEXC, tab], ignore_index=True)

print(tabEXC)

for root, dirs, files in os.walk(caminho_original):
    for file in files:
        path = os.path.join(root, file)
        if ".txt" in file:
            if "MOV" in file:  # definir a extensão do aqruivo
                print(path)
                tab = pd.read_table(path, sep=";", encoding='utf-8', usecols=columns)
                # tab = tab[tab['uf'] == 52]
                tab["Tipo"] = 'MOV'
                tabMOV = pd.concat([tabMOV, tab], ignore_index=True)

print(tabMOV)
# Os valores do saldo movimentação serão multiplicados por (-1) pois exclusões de admissões
# diminuem o saldo e exclusões de desligamentos aumentam o saldo.
tabEXC['saldomovimentação'] = tabEXC['saldomovimentação'].map({1: -1, -1: 1})

# Use concat para empilhar múltiplos DataFrames em cima
dataCAGED = pd.concat([tabEXC, tabMOV, tabFOR], ignore_index=True, axis=0)
dataCAGED.to_csv(f'base/caged_full_{datetime.today().strftime("%Y%m%d")}.csv', sep=';', index=False, encoding='utf-8')

exit()
# Deletar a coluna UF
dataCAGED = dataCAGED.drop(columns=['uf'])
dataCAGED["UF"] = "GO"

# Criar uma coluna igual a competência mov
dataCAGED["Competência"] = dataCAGED["competênciamov"]

dataCAGED['competênciamov'] = dataCAGED['competênciamov'].astype(str)
dataCAGED['Data'] = dataCAGED['competênciamov'] + "01"

# Alterando o tipo de "idade" de "int64" para "float"
dataCAGED["idade"] = pd.to_numeric(dataCAGED["idade"], errors='coerce')

# Para o BI o tipo de movimentação foi agrupado e codificada em 3 categorias
# Os tipos de movimentação são os seguintes, 'Admitido', 'Desligado' e 'Saldo'
# Saldo = Admitido - Desligado
conditions = [
    (dataCAGED['tipomovimentação'].isin([10, 20, 25, 35, 70, 97])),
    (dataCAGED['tipomovimentação'].isin([31, 32, 33, 40, 43, 45, 50, 60, 80, 90, 98, 99]))
]
values = ['Admissões', 'Desligamentos']

dataCAGED['Movimentacao'] = np.select(conditions, values, default=np.nan)

# Codificacar a variável sexo com as informações do arquivo layout
dataCAGED['Sexo'] = dataCAGED['sexo'].map({1: 'Masculino', 3: 'Feminino', 9: 'Não Identificado'})

# Deletar a coluna sexo original
dataCAGED = dataCAGED.drop(columns=['sexo'])

# Codificacar a variável raçacor com as informações do arquivo layout
dataCAGED['Raça/Cor'] = dataCAGED['raçacor'].map(
    {1: '1-Branca', 2: '2-Preta', 3: '3-Parda', 4: '4-Amarela', 5: '5-Indígena', 6: '6-Não informada',
     9: '7-Não Identificado'})

# Deletar a coluna raçacor original
dataCAGED = dataCAGED.drop(columns=['raçacor'])

# Codificacar a variável grau de instrução com as informações do arquivo layout
dataCAGED['Escolaridade'] = dataCAGED['graudeinstrução'].map({
    1: "1-Analfabeto",
    2: "2-Até 5ª Incompleto",
    3: "3-5ª Completo Fundamental",
    4: "4-6ª a 9ª Fundamental",
    5: "5-Fundamental Completo",
    6: "6-Médio Incompleto",
    7: "7-Médio Completo",
    8: "8-Superior Incompleto",
    9: "10-Superior Completo",
    10: "11-Mestrado",
    11: "12-Doutorado",
    80: "13-Pós-Graduação completa",
    99: "14-Não Identificado",
})

# Deletar a coluna grau de instrução original
dataCAGED = dataCAGED.drop(columns=['graudeinstrução'])

# Codificacar a variável categoria
dataCAGED['Categoria'] = dataCAGED['categoria'].map({
    101: "1-Empregado - Geral, inclusive o empregado público da administração direta ou indireta contratado pela CLT",
    102: "2-Empregado - Trabalhador rural por pequeno prazo da Lei 11.718/2008",
    103: "3-Empregado - Aprendiz",
    104: "4-Empregado - Doméstico",
    105: "5-Empregado - Contrato a termo firmado nos termos da Lei 9.601/1998",
    106: "6-Trabalhador temporário - Contrato nos termos da Lei 6.019/1974",
    107: "7-Empregado - Contrato de trabalho Verde e Amarelo - sem acordo para antecipação mensal da multa rescisória do FGTS",
    108: "8-Empregado - Contrato de trabalho Verde e Amarelo - com acordo para antecipação mensal da multa rescisória do FGTS",
    111: "9-Empregado - Contrato de trabalho intermitente",
    999: "10-Não Identificado"
})

# Deletar a coluna categoria
dataCAGED = dataCAGED.drop(columns=['categoria'])

# Codificacar a variável tipoestabelecimento
dataCAGED['TipoEstabelecimento'] = dataCAGED['tipoestabelecimento'].map({
    1: "1-CNPJ",
    3: "2-CAEPF(Cadastro de Atividade Econômica de Pessoa Física)",
    4: "3-CNO(Cadastro Nacional de Obra)",
    5: "4-CEI(CAGED)",
    9: "5-Não Identificado"
})

# Deletar a coluna tipoestabelecimento
dataCAGED = dataCAGED.drop(columns=['tipoestabelecimento'])

# Codificacar a variável tipodedeficiência
dataCAGED['TipodeDeficiência'] = dataCAGED['tipodedeficiência'].map({
    0: "1-Não Deficiente",
    1: "2-Física",
    2: "3-Auditiva",
    3: "4-Visual",
    4: "5-Intelectual (Mental)",
    5: "6-Múltipla",
    6: "7-Reabilitado",
    9: "8-Não Identificado"
})

# Deletar a coluna tipodedeficiência
dataCAGED = dataCAGED.drop(columns=['tipodedeficiência'])

# Codificacar a variável tamestabjan
dataCAGED['Faixa_de_Emprego_no_Início_Janeiro'] = dataCAGED['tamestabjan'].map({
    1: "1-Zero",
    2: "2-De 1 a 4",
    3: "3-De 5 a 9",
    4: "4-De 10 a 19",
    5: "5-De 20 a 49",
    6: "6-De 50 a 99",
    7: "7-De 100 a 249",
    8: "8-De 250 a 499",
    9: "9-De 500 a 999",
    10: "10-1000 ou Mais",
    99: "11-Ignorado",
    98: "12-Inválido",
    97: "13-Não se Aplica",
    90: "14-Não Identificado"
})

# Deletar a coluna tamestabjan
dataCAGED = dataCAGED.drop(columns=['tamestabjan'])

# Codificacar a variável indtrabintermitente
dataCAGED['IndtrabIntermitente'] = dataCAGED['indtrabintermitente'].map({0: 'Não', 1: 'Sim', 9: 'Não Identificado'})

# Codificacar a variável indtrabparcial
dataCAGED['indtrabparcial'] = dataCAGED['indtrabparcial'].map({0: 'Não', 1: 'Sim', 9: 'Não Identificado'})

# Codificacar a variável indicadoraprendiz
dataCAGED['indicadoraprendiz'] = dataCAGED['indicadoraprendiz'].map({0: 'Não', 1: 'Sim', 9: 'Não Identificado'})

# Codificacar a variável origemdainformação
dataCAGED['origemdainformação'] = dataCAGED['origemdainformação'].map(
    {1: '1-eSocial', 2: '2-CAGED', 3: '3-EmpregadoWEB'})

# Codificacar a variável indicadordeforadoprazo
dataCAGED['indicadordeforadoprazo'] = dataCAGED['indicadordeforadoprazo'].map({0: 'Não', 1: 'Sim'})

# Importando os tipos cbo2002ocupação do arquivo layout
tipomovimentação = pd.read_excel('D:/CAGED/Dados_Auxiliares_Goias/Layout Não-identificado Novo Caged Movimentação.xlsx',
                                 sheet_name="tipomovimentação")
tipomovimentação.columns = ['Codigotipo', 'TipoMovimentação']  # Renomear as colunas do tabela cbo2002ocupação

# Fazer o merge dos dois dataframe
dataCAGED = pd.merge(dataCAGED, tipomovimentação, how='left', left_on=['tipomovimentação'], right_on=['Codigotipo'])

# Deletar a coluna grau de instrução original
dataCAGED = dataCAGED.drop(columns=['Codigotipo'])
dataCAGED = dataCAGED.drop(columns=['tipomovimentação'])

# Importando os tipos cbo2002ocupação do arquivo layout
cbo2002ocupação = pd.read_excel('D:/CAGED/Dados_Auxiliares_Goias/Layout Não-identificado Novo Caged Movimentação.xlsx',
                                sheet_name="cbo2002ocupação")
cbo2002ocupação.columns = ['Codigo', 'CBO']  # Renomear as colunas do tabela cbo2002ocupação

# Fazer o merge dos dois dataframe
dataCAGED = pd.merge(dataCAGED, cbo2002ocupação, how='left', left_on=['cbo2002ocupação'], right_on=['Codigo'])

# Deletar a coluna Codigo 
dataCAGED = dataCAGED.drop(columns=['Codigo'])

# pd.cut Fatiamento da idade 
bin_labels = labels = ["1-Até 17 anos", "2-De 18 a 24 anos", "3-De 25 a 29 anos", "4-De 30 a 39 anos",
                       "5-De 40 a 49 anos", "6-De 50 a 64 anos", "7-De 65 anos ou mais"]
dataCAGED['Faixa_idade'] = pd.cut(dataCAGED['idade'], bins=[0, 17, 24, 29, 39, 49, 64, 1000], labels=bin_labels)

# Deletar a coluna idade
dataCAGED = dataCAGED.drop(columns=['idade'])

# Alterando o tipo de "horascontratuais" de "int64" para "float"
# Parte está em número e parte em texto, primeiro irei converter tudo em texto.
dataCAGED['horascontratuais'] = dataCAGED['horascontratuais'].astype(str)
dataCAGED['horascontratuais'] = dataCAGED['horascontratuais'].str.replace(',', '.')

# dataCAGED['horascontratuais'] = dataCAGED['horascontratuais'].apply(lambda x: str(x).replace(',','.'))
dataCAGED['horascontratuais'] = pd.to_numeric(dataCAGED['horascontratuais'], errors='coerce')

# pd.cut Fatiamento das horas traballhadas 
bin_labels = labels = ["1-Até 14 horas", "2-De 15 a 39 horas", "3-De 40 a 44 horas", "4-De 45 a 48 horas",
                       "5-De 49 ou mais horas"]
dataCAGED['Faixa_de_horas_trabalhadas'] = pd.cut(dataCAGED['horascontratuais'], bins=[-1, 14, 39, 44, 48, 1000],
                                                 labels=bin_labels)

# Deletar a coluna idade
dataCAGED = dataCAGED.drop(columns=['horascontratuais'])

# Importando os tipos regiões de turismo
regioesTurismo = pd.read_csv('D:/CAGED/Dados_Auxiliares_Goias/dRegioesTuristicas.csv', delimiter=";", encoding="ISO-8859-1")
regioesTurismo = regioesTurismo.drop(columns=['Regiao_Turistica'])

# Fazer o merge dos dois dataframe
dataCAGED = pd.merge(dataCAGED, regioesTurismo, how='left', left_on=['município'], right_on=['CDIBGE6D'])

# Deletar a coluna Codigo original
dataCAGED = dataCAGED.drop(columns=['CDIBGE6D'])

# Criar uma coluna com valor unitário pois cada linha do dataframe equivale a um trabalhador
dataCAGED["Vinculos"] = 1

Cnae = pd.read_csv(
    'D:/CAGED/Dados_Auxiliares_Goias/CNAE_Subclasses_2_3.csv',
    sep=";", encoding='utf-8-sig')

# Fazer o merge dos dois dataframe
dataCAGED = pd.merge(dataCAGED, Cnae, how='left', left_on=['subclasse'], right_on=['Subclasse'])

dataCAGED = dataCAGED.drop(columns=['competênciamov'])
dataCAGED = dataCAGED.drop(columns=['subclasse'])
dataCAGED = dataCAGED.drop(columns=['município'])
dataCAGED = dataCAGED.drop(columns=['cbo2002ocupação'])

dataCAGED.rename(columns={'salário': 'Salário', 'indtrabintermitente': 'IndtrabIntermitente',
                          'indtrabparcial': 'IndtrabParcial', 'saldomovimentação': 'SaldoMovimentação',
                          'saldomovimentação': 'SaldoMovimentação', 'horascontratuais': 'HorasContratuais',
                          'idade': 'Idade', 'indicadordeforadoprazo': 'IndicadordeForadoPrazo',
                          'indicadoraprendiz': 'IndicadorAprendiz', 'origemdainformação': 'OrigemdaInformação',
                          'Nome_Mun': 'NomeMunicipio'}, inplace=True)

dataCAGED['Salário'] = dataCAGED['Salário'].astype(str)
dataCAGED['Salário'] = dataCAGED['Salário'].str.replace(',', '.')
dataCAGED['Salário'] = pd.to_numeric(dataCAGED['Salário'], errors='coerce')

dataCAGED['Salário'].unique()
print(dataCAGED.columns)

# Salvar o parquet

dataCAGED.to_csv(f'resultado/Novo_Caged_EC_{datetime.today().strftime("%Y%m%d")}.csv', index=False, sep=';', header=True, encoding='utf-8-sig')
dataCAGED.to_csv(f'Z:/GEDE/Desenvolvimento/Projetos Antigos/PaineisGoiasIndicadores/BI_Turismo_Observatorio/resultado/Novo_Caged_EC_{datetime.today().strftime("%Y%m%d")}.csv', index=False, sep=';', header=True, encoding='utf-8-sig')
