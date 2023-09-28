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
    df_final = pd.DataFrame()
    df2 = df2[df2['Status / Aba'].str.contains('STA')]
    df2 = df2[df2['CLASSIFICACAO'] != 'Saldo [cat. Saldo]']
    df2.dropna(subset = ['DATA'], inplace=True)
    df_expat = df2[df2['CLASSIFICACAO'] == 'Expatriação (operação) [cat. Expatriação]']
    df_tarifas = df2[df2['CLASSIFICACAO'].str.contains('Tarifa', na=False)]
    df_expat['DATA'] = df_expat['DATA'].astype(np.int64)
    df_expat['VALOR'] = df_expat['VALOR'].astype(np.float64)
    df_expat.insert(22,'Movimento', value = '1')
    df_tarifas['DATA'] = df_tarifas['DATA'].astype(np.int64)
    df_tarifas['VALOR'] = df_tarifas['VALOR'].astype(np.float64)
    df_tarifas.insert(22,'Movimento', value = '1')
    df2 = df2[df2['CLASSIFICACAO'] != 'Expatriação (operação) [cat. Expatriação]']
    tarifas = ['Tarifa (sem class.) [cat. Custos Gerais]', 'Tarifa Boletos [cat. Custos Cashin]' ,'Tarifa Cashin PIX [cat. Custos Cashin]',
                'Tarifa Cashin (outras) [cat. Custos Cashin]', 'Tarifa Cashout PIX [cat. Custos Cashout]' ,'Tarifa Cashout (outras) [cat. Custos Cashout]',
                'Tarifa CNAB [cat. Custos Cashout]', 'Tarifa Conta [cat. Custos Gerais]', 'Tarifa DOC/TED [cat. Custos Cashout]' ,'Tarifa Selenium [cat. Custos Cashin]',
                'Tarifa TEF [cat. Custos Cashin]','Tarifa Transf. (saídas) [cat. Custos Cashout]']
    for tarifa in tarifas:
        df2 = df2[df2['CLASSIFICACAO'] != tarifa]
    df2['DATA'] = df2['DATA'].astype(np.int64)
    df2['VALOR'] = df2['VALOR'].astype(np.float64)
    df2.insert(22,'Movimento', value = '0')
    datas = list(dict.fromkeys(list(df2['DATA'])))
    for data in datas:
        df_auxiliar = df2[df2['DATA'] == data]
        descrições = list(dict.fromkeys(list(df_auxiliar['DESCRIÇÃO'])))
        for descrição in descrições:
            df_recebido = df_auxiliar[df_auxiliar['DESCRIÇÃO'] == descrição]
            recebido = df_recebido['VALOR'].sum()
            movimentos = len(df_recebido.index)
            linha_padrão = df_auxiliar.iloc[:1]
            linha_padrão['CLASSIFICACAO'] = linha_padrão['CLASSIFICACAO'].apply(lambda x: str(x).replace(str(list(linha_padrão['CLASSIFICACAO'])[0]), 'Linha Python'))
            linha_padrão['DESCRIÇÃO'] = linha_padrão['DESCRIÇÃO'].apply(lambda x: str(x).replace(str(list(linha_padrão['DESCRIÇÃO'])[0]), str(descrição)))
            linha_padrão['DESCRIÇÃO'] = linha_padrão['DESCRIÇÃO'].apply(lambda x: str(x).replace('nan', ''))
            linha_padrão['NUM DOCTO'] = linha_padrão['NUM DOCTO'].apply(lambda x: str(x).replace(str(list(linha_padrão['NUM DOCTO'])[0]), ''))
            linha_padrão['C / D'] = linha_padrão['C / D'].apply(lambda x: str(x).replace(str(list(linha_padrão['C / D'])[0]), ''))
            linha_padrão['SALDO'] = linha_padrão['SALDO'].apply(lambda x: str(x).replace(str(list(linha_padrão['SALDO'])[0]), ''))
            linha_padrão['NOME'] = linha_padrão['NOME'].apply(lambda x: str(x).replace(str(list(linha_padrão['NOME'])[0]), ''))
            linha_padrão['CONTA'] = linha_padrão['CONTA'].apply(lambda x: str(x).replace(str(list(linha_padrão['CONTA'])[0]), ''))
            linha_padrão['AGENCIA'] = linha_padrão['AGENCIA'].apply(lambda x: str(x).replace(str(list(linha_padrão['AGENCIA'])[0]), ''))
            linha_padrão['DOCTO CLIENTE'] = linha_padrão['DOCTO CLIENTE'].apply(lambda x: str(x).replace(str(list(linha_padrão['DOCTO CLIENTE'])[0]), ''))
            linha_padrão['GENERICO 1'] = linha_padrão['GENERICO 1'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 1'])[0]), ''))
            linha_padrão['GENERICO 2'] = linha_padrão['GENERICO 2'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 2'])[0]), ''))
            linha_padrão['GENERICO 3'] = linha_padrão['GENERICO 3'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 3'])[0]), ''))
            linha_padrão['GENERICO 4'] = linha_padrão['GENERICO 4'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 4'])[0]), ''))
            linha_padrão['Movimento'] = linha_padrão['Movimento'].apply(lambda x: str(x).replace(str(list(linha_padrão['Movimento'])[0]), str(movimentos)))
            linha_padrão['VALOR'] = linha_padrão['VALOR'].apply(lambda x: str(x).replace(str(list(linha_padrão['VALOR'])[0]), str(recebido)))
            linha_padrão['Movimento'] = linha_padrão['Movimento'].astype(np.int64)
            linha_padrão['VALOR'] = linha_padrão['VALOR'].astype(np.float64)
            if df_final.empty:
                df_final = linha_padrão
            else:
                df_final = pd.concat([df_final,linha_padrão], ignore_index=True)
    if df_final.empty:
        df_final = df_expat
    else:
        df_final = pd.concat([df_final,df_expat], ignore_index=True)
    if df_final.empty:
        df_final = df_tarifas
    else:
        df_final = pd.concat([df_final,df_tarifas], ignore_index=True)
    #print(df_final)
    df_final['Movimento'] = df_final['Movimento'].astype(np.int64)
    #linha_padrão['DESCRIÇÃO'] = linha_padrão['DESCRIÇÃO'].apply(lambda x: str(x).replace('nan', ''))
    df_final.to_csv(caminho_parcial+agente+".csv", encoding='utf-8-sig', index = False)

if __name__ == '__main__':
    inicio = time.perf_counter()
    usuario = os.getcwd().split('\\')[2]
    caminho_final = "C:\\Users\\"+usuario+"\\Projetos Python\\Análises\\StarkBank\\StarkFinal.xlsx"
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
