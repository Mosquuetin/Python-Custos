import pandas as pd
import os

#caminho_inicial = os.getcwd()+"\Auxiliar\Cronograma.xlsx"
#caminho_final = os.getcwd()+"\Análises\Análises Gerais\\"
caminho_inicial = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Auxiliar\Cronograma.xlsx"
caminho_final = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Análises\Análises Gerais\\"

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
df3 = pd.DataFrame()
df5 = pd.DataFrame()
df9 = pd.DataFrame()
df13 = pd.DataFrame()
for agente in agentes:
    print('Iniciando '+ agente)
    caminho = caminhos[i]
    i = i + 1
    df1 = pd.read_csv(caminho, low_memory=False, usecols=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22])
    df2 = df1
    df2 = df2[df2['CLASSIFICACAO'].str.contains('cat. Custos Adm', na = False)]
    if df3.empty:
        df3 = df2
    else:
        df3 = pd.concat([df3,df2])
    #Fim do Pagamentos ADM, Início do Relevantes Fora
    df4 = df1
    df4 = df4[df4['CLASSIFICACAO'].str.contains('cat. Outros', na = False)]
    if df5.empty:
        df5 = df4
    else:
        df5 = pd.concat([df5,df4])
    df6 = df1
    df6 = df6[df6['CLASSIFICACAO'].str.contains('cat. Reportado', na = False)]
    if df5.empty:
        df5 = df6
    else:
        df5 = pd.concat([df5,df6])
    df7 = df1
    df7 = df7[df7['CLASSIFICACAO'].str.contains('cat. Não Localizado', na = False)]
    if df5.empty:
        df5 = df7
    else:
        df5 = pd.concat([df5,df7])
    #Fim do Relevantes Fora, Início Classificação
    df8 = df1
    df8 = df8[df8['CLASSIFICACAO'].str.contains("Boletos [cat. Entrada Painel]", na = False)]
    if df9.empty:
        df9 = df8
    else:
        df9 = pd.concat([df9,df8])
    df15 = df1
    df15 = df15[df15['CLASSIFICACAO'].str.contains('Erro Payout', na = False)]
    if df9.empty:
        df9 = df15
    else:
        df9 = pd.concat([df9,df15])
    df16 = df1
    df16 = df16[df16['CLASSIFICACAO'].str.contains('Credito Cashin', na = False)]
    if df9.empty:
        df9 = df16
    else:
        df9 = pd.concat([df9,df16])
    df17 = df1
    df17 = df17[df17['CLASSIFICACAO'].str.contains('TEF', na = False)]
    if df9.empty:
        df9 = df17
    else:
        df9 = pd.concat([df9,df17])
    df10 = df1
    df10 = df10[df10['CLASSIFICACAO'].str.contains('Tarifa', na = False)]
    if df9.empty:
        df9 = df10
    else:
        df9 = pd.concat([df9,df10])
    #Fim do Classificação, Início Transf e Expat
    df12 = df1
    df12 = df12[df12['CLASSIFICACAO'].str.contains('Transfer', na = False)]
    if df13.empty:
        df13 = df12
    else:
        df13 = pd.concat([df13,df12])
    df14 = df1
    df14 = df14[df14['CLASSIFICACAO'].str.contains('Expat', na = False)]
    if df13.empty:
        df13 = df14
    else:
        df13 = pd.concat([df13,df14])
print('Salvando Arquivo')
df3.to_csv(caminho_final+"BR.Pgto ADM.csv", index = False)
df5.to_csv(caminho_final+"BR.Relevantes Fora.csv", index = False)
df9.to_csv(caminho_final+"BR.Classificacao.csv", index = False)
df13.to_csv(caminho_final+"BR.Transf e Expat.csv", index = False)