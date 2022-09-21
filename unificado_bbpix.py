import pandas as pd
import os

#caminho_inicial = os.getcwd()+"\Auxiliar\Cronograma.xlsx"
#caminho_final = os.getcwd()+"\Análises\BB Pix\\"
caminho_inicial = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Auxiliar\Cronograma.xlsx"
caminho_final = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Análises\BB Pix\\"

df = pd.read_excel(caminho_inicial, sheet_name="Parâmetros Python")
aba_inicial = list(df['Aba Inicial'])
aba_auxiliar = list(df['Aba Auxiliar'])
aba_inicial = aba_inicial[0]
aba_auxiliar = aba_auxiliar[0]
df = pd.read_excel(caminho_inicial, sheet_name=aba_inicial)
print('Arquivo aberto')
agentes = list(df['Agente'])
arquivos = list(df['Arquivo'])
caminhos = list(df['Caminho'])
df = pd.read_excel(caminho_inicial, sheet_name=aba_auxiliar)
print(df)
print('Executando Loop')
i = 0
x = 1
df3 = pd.DataFrame()
for agente in agentes:
    df1 = df[df['Agente'] == agente]
    abas = list(df1['Nome da Aba'])
    aba = abas[0]
    caminho = caminhos[i]
    arquivo = arquivos[i]
    i = i + 1
    print('Lendo aba ' + aba + ' de ' + agente)
    df2 = pd.read_csv(caminho, low_memory=False, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24])
    df2 = df2[df2['Status / Aba'] == aba]
    df2 = df2[df2['DESCRIÇÃO'].str.contains('Recebido')]
    if  len(pd.concat([df3,df2]).index) > 1050000:
        print('Salvando Arquivo')
        df3.to_csv(caminho_final+"BB"+str(x)+".csv", index = False)
        x = x + 1
        df3 = df2
    else:
        if df3.empty:
            df3 = df2
        else:
            df3 = pd.concat([df3,df2])
if len(df3.index) > 1:
    print('Salvando Último Arquivo Com Menos Linhas')
    df3.to_csv(caminho_final+"STA"+str(x)+".csv", index = False)