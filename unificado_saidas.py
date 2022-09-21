import pandas as pd
import os

#caminho_inicial = os.getcwd()+"\Auxiliar\Cronograma.xlsx"
#caminho_final = os.getcwd()+"\Análises\Análises Gerais\Saidas.csv"
caminho_inicial = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Auxiliar\Cronograma.xlsx"
caminho_final = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Análises\Análises Gerais\Saidas.csv"

df = pd.read_excel(caminho_inicial, sheet_name="Parâmetros Python")
aba_inicial = list(df['Aba Inicial'])
aba_inicial = aba_inicial[0]
df = pd.read_excel(caminho_inicial, sheet_name=aba_inicial)
print('Arquivo aberto')
agentes = list(df['Agente'])
arquivos = list(df['Arquivo'])
caminhos = list(df['Caminho'])
print(df)
print('Executando Loop')
i = 0
df1 = pd.DataFrame()
for agente in agentes:
    print('Iniciando '+ agente)
    caminho = caminhos[i]
    arquivo = arquivos[i]
    i = i + 1
    print('Lendo ' + agente)
    df2 = pd.read_csv(caminho, low_memory=False, usecols=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22])
    df2 = df2[df2['C / D'] == 'D']
    df2 = df2[df2['Status / Aba'] != 'STA_1']
    df2 = df2[df2['Status / Aba'] != 'STA_2']
    df2 = df2[df2['Status / Aba'] != 'STA_3']
    if df1.empty:
        df1 = df2
    else:
        df1 = pd.concat([df1,df2])
print('Salvando Arquivo')
df1.to_csv(caminho_final, index = False)
