import pandas as pd
import msoffcrypto
import io
import os
import numpy as np

caminho_inicial = os.getcwd()+"\Auxiliar\Cronograma.xlsx"
caminho_final = os.getcwd()+"\Custos Consolidados\\"

df = pd.read_excel(caminho_inicial, sheet_name="Parâmetros Python")
aba_inicial = list(df['Aba Inicial'])
aba_auxiliar = list(df['Aba Auxiliar'])
pessoa = list(df['Quem?'])
aba_inicial = aba_inicial[0]
aba_auxiliar = aba_auxiliar[0]
pessoa = pessoa[0]
df = pd.read_excel(caminho_inicial, sheet_name=aba_inicial)
print('Arquivo aberto')
df = df[df['Quem'] == pessoa]
print(df)
agentes = list(df['Agente'])
arquivos = list(df['Arquivo'])
caminhos = list(df['Caminho'])
df = pd.read_excel(caminho_inicial, sheet_name=aba_auxiliar)
print(df)
print('Executando Loop')
i = 0
for agente in agentes:
    print('Iniciando '+ agente)
    df1 = df[df['Agente'] == agente]
    abas = list(df1['Nome da Aba'])
    caminho = caminhos[i]
    arquivo = arquivos[i]
    i = i + 1
    print(abas)
    x = 0
    df2 = pd.DataFrame()
    abertura_excel = io.BytesIO()
    with open(caminho, 'rb') as abertura:
        excel = msoffcrypto.OfficeFile(abertura)
        excel.load_key(('apc2016'))
        excel.decrypt(abertura_excel)
    for aba in abas:
        print('Lendo aba ' + aba + ' de ' + agente)
        responsaveis = list(df1['Responsável'])
        grupos = list(df1['País / Grupo'])
        bancos = list(df1['Banco'])
        agencias = list(df1['Agência'])
        contas = list(df1['Conta'])
        responsavel = responsaveis[x]
        grupo = grupos[x]
        banco = bancos[x]
        agencia = agencias[x]
        conta = contas [x]
        df3 = pd.read_excel(abertura_excel, sheet_name = aba, header = 5, usecols = "A:AS", na_filter=False)
        df3.insert(0,'Conta', value = conta)
        df3.insert(0,'Agência', value = agencia)
        df3.insert(0,'Banco', value = banco)
        df3.insert(0,'Agente', value = agente)
        df3.insert(0,'País / Grupo', value = grupo)
        df3.insert(0,'Status / Aba', value = aba)
        df3.insert(0,'Responsável', value = responsavel)
        x = x + 1
        if df2.empty:
            df2 = df3
        else:
            df2 = pd.concat([df2,df3]) 
    del abertura_excel          
    print('Compilando Base '+ agente)
    print('Salvando Arquivo')
    df2.to_csv(caminho_final+arquivo+".txt", index = False)
