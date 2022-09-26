import pandas as pd
import os
import numpy as np

caminho_inicial = os.getcwd()+"\Auxiliar\Cronograma.xlsx"
caminho_final = os.getcwd()+"\Análises\Análises Gerais\TotalExtratos.xlsx"


df = pd.read_excel(caminho_inicial, sheet_name="Parâmetros Python")
aba_inicial = list(df['Aba Inicial'])
aba_inicial = aba_inicial[0]
df = pd.read_excel(caminho_inicial, sheet_name=aba_inicial)
print('Arquivo aberto')
agentes = list(df['Agente'])
caminhos = list(df['Caminho'])
print(df)
print('Executando Loop')
i = 0
df1 = pd.DataFrame()
for agente in agentes:
    caminho = caminhos[i]
    print('Iniciando ' + agente)
    df2 = pd.read_csv(caminho, low_memory=False, usecols=[1,3,11,12])
    abas = list(dict.fromkeys(list(df2['Status / Aba'])))
    i = i + 1
    for aba in abas:
        print('Lendo '+ aba + ' de ' + agente)
        df3 = df2[df2['Status / Aba'] == aba]
        df3 = df3[df3['C / D'] == "D"]
        df3['VALOR'] = df3['VALOR'].astype(np.float64)
        df3 = df3['VALOR']
        pd.to_numeric(df3, downcast="float")
        df3 = df3.sum()
        somad = df3
        df3 = df2[df2['Status / Aba'] == aba]
        df3 = df3[df3['C / D'] == "C"]
        df3['VALOR'] = df3['VALOR'].astype(np.float64)
        df3 = df3['VALOR']
        pd.to_numeric(df3, downcast="float")
        df3 = df3.sum()
        somac = df3
        df3 = {'Agente': [agente],
                'Aba': [aba],
                'Soma Credito': [somac],
                'Soma Debito': [somad]}
        df3 = pd.DataFrame(df3)
        if df1.empty:
            df1 = df3
        else:
            df1 = pd.concat([df1,df3])
    print(df1)
print('Salvando Arquivo')
df1.to_excel(caminho_final, index = False)
