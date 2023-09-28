import pandas as pd
import os
import numpy as np
import time
import concurrent.futures
import glob
import shutil
pd.options.mode.chained_assignment = None

def etl_txt(arquivos,caminho_parcial):
    agente = arquivos.split('\\')[8]
    agente = agente[:-4]
    agente = agente.split('Extratos - ')[1]
    print('Iniciando '+ agente)
    df1 = pd.DataFrame()
    df2 = pd.read_csv(arquivos, encoding='utf-8-sig',  low_memory=False, usecols=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22])

    abas = list(dict.fromkeys(list(df2['Status / Aba'])))
    for aba in abas:
        df3 = df2[df2['Status / Aba'] == aba]
        df3['C / D'] = df3['C / D'].astype(str)
        df3 = df3[df3['C / D'].str.contains('D', na = False)]
        df3['VALOR'] = df3['VALOR'].astype(np.float64)
        df3 = df3['VALOR']
        pd.to_numeric(df3, downcast="float")
        df3 = df3.sum()
        somad = df3
        df3 = df2[df2['Status / Aba'] == aba]
        df3['C / D'] = df3['C / D'].astype(str)
        df3 = df3[df3['C / D'].str.contains('C', na = False)]
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
  
    df1.to_csv(caminho_parcial+agente+".csv", encoding='utf-8-sig', index = False)

if __name__ == '__main__':
    inicio = time.perf_counter()
    usuario = os.getcwd().split('\\')[2]
    caminho_final = "C:\\Users\\"+usuario+"\\Projetos Python\\Análises\\Análises Gerais\\TotalExtratos.xlsx"
    caminho_parcial = "C:\\Users\\"+usuario+"\\Projetos Python\\temp\\"
    arquivos = "C:\\Users\\"+usuario+"\\Projetos Python\\Custos Consolidados\\"

    csvs = glob.glob(caminho_parcial+"*csv*")
    if not csvs:
        print('Pasta temp incialmente vazia')
    else:
        print('Deletando arquivos da pasta temp')
        for csv in csvs:
            os.remove(csv)

    arquivos = glob.glob(arquivos+"*txt*")
    print('Executando Loop')

    qtd = len(arquivos)
    caminho_parcial = [caminho_parcial] * qtd

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(etl_txt,arquivos,caminho_parcial)

    csvs = glob.glob(caminho_parcial[0]+"*csv*")
    df = pd.DataFrame()
    if not csvs:
        pass
    else:
        for csv in csvs:
            df1 = pd.read_csv(csv, encoding='utf-8-sig', low_memory=False)
            if df.empty:
                df = df1
            else:
                df = pd.concat([df,df1])
        print('Salvando aqruivo final')
        df.to_excel(caminho_final, index = False)
    
    fim = time.perf_counter()
    print((time.strftime("%H:%M:%S", time.gmtime(fim-inicio))))