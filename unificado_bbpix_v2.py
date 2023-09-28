import pandas as pd
import os
import numpy as np
import time
import concurrent.futures
import glob
import shutil
pd.options.mode.chained_assignment = None

def etl_txt(agentes,caminhos,caminho_parcial,df,caminho_tarifa,df_tarifas):
    pd.options.mode.chained_assignment = None 
    df_aba = df[df['Agente'] == agentes]
    aba = list(df_aba['Nome da Aba'])[0]
    df_final = pd.DataFrame()
    print('Lendo aba ' + aba + ' de ' + agentes)
    df2 = pd.read_csv(caminhos, low_memory=False, usecols=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22])
    df2 = df2[df2['Status / Aba'] == aba]
    df2 = df2[df2['DESCRIÇÃO'].str.contains('Recebido', na = False)]
    df2 = df2[df2['CLASSIFICACAO'] != 'Transfer Msm Tit (ok mesma plan) [cat. Transf. Contas (msm tit - na plan)]']
    df2['DATA'] = df2['DATA'].astype(np.int64)
    df2['VALOR'] = df2['VALOR'].astype(np.float64)
    df2.insert(0,'Auxiliar', value = df2['Agente']+df2['DATA'].astype(str))
    df2.insert(23,'Movimento', value = '0')
    df2.insert(24,'Recebido', value = '0')
    datas = list(dict.fromkeys(list(df2['DATA'])))
    for data in datas:
        df_auxiliar = df2[df2['DATA'] == data]
        df_verconcil = df_auxiliar[df_auxiliar['CLASSIFICACAO'] != 'Tarifa Cashin PIX [cat. Custos Cashin]']
        recebido = df_verconcil['VALOR'].sum()
        df_custo = df_auxiliar[df_auxiliar['CLASSIFICACAO'] == 'Tarifa Cashin PIX [cat. Custos Cashin]']
        auxiliar = list(df_auxiliar['Auxiliar'])[0]
        auxiliar = df_tarifas[df_tarifas['Auxiliar'] == auxiliar]
        tarifa = list(auxiliar['Tarifa'])[0]
        unidade = list(auxiliar['UN/%'])[0]
        minimo = list(auxiliar['MIN'])[0]
        maximo = list(auxiliar['MAX'])[0]
        if unidade == "%":
            df_verconcil['VALOR'] = df_verconcil['VALOR'] * -tarifa
            df_verconcil['VALOR'] = df_verconcil['VALOR'].round(decimals=2)
            df_verconcil['VALOR'] = np.where(df_verconcil['VALOR'] < maximo, maximo, df_verconcil['VALOR'])
            df_verconcil['VALOR'] = np.where(df_verconcil['VALOR'] > minimo, minimo, df_verconcil['VALOR'])
            movimentos = len(df_verconcil.index)
            tarifa = round(df_verconcil['VALOR'].sum(), 2)
            linha_padrão = df_auxiliar.iloc[:1]
            linha_padrão['CLASSIFICACAO'] = linha_padrão['CLASSIFICACAO'].apply(lambda x: str(x).replace(str(list(linha_padrão['CLASSIFICACAO'])[0]), 'Tarifa Python'))
            linha_padrão['DESCRIÇÃO'] = linha_padrão['DESCRIÇÃO'].apply(lambda x: str(x).replace(str(list(linha_padrão['DESCRIÇÃO'])[0]), 'Tarifa Python'))
            linha_padrão['NUM DOCTO'] = linha_padrão['NUM DOCTO'].apply(lambda x: str(x).replace(str(list(linha_padrão['NUM DOCTO'])[0]), ''))
            linha_padrão['C / D'] = linha_padrão['C / D'].apply(lambda x: str(x).replace(str(list(linha_padrão['C / D'])[0]), ''))
            linha_padrão['SALDO'] = linha_padrão['SALDO'].apply(lambda x: str(x).replace(str(list(linha_padrão['SALDO'])[0]), ''))
            linha_padrão['NOME'] = linha_padrão['NOME'].apply(lambda x: str(x).replace(str(list(linha_padrão['NOME'])[0]), ''))
            linha_padrão['CONTA'] = linha_padrão['CONTA'].apply(lambda x: str(x).replace(str(list(linha_padrão['CONTA'])[0]), ''))
            linha_padrão['AGENCIA'] = linha_padrão['AGENCIA'].apply(lambda x: str(x).replace(str(list(linha_padrão['AGENCIA'])[0]), ''))
            linha_padrão['DOCTO CLIENTE'] = linha_padrão['DOCTO CLIENTE'].apply(lambda x: str(x).replace(str(list(linha_padrão['DOCTO CLIENTE'])[0]), ''))
            linha_padrão['GENERICO 1'] = linha_padrão['GENERICO 1'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 1'])[0]), ''))
            linha_padrão['GENERICO 2'] = linha_padrão['GENERICO 2'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 2'])[0]), ''))
            linha_padrão['Movimento'] = linha_padrão['Movimento'].apply(lambda x: str(x).replace(str(list(linha_padrão['Movimento'])[0]), str(movimentos)))
            linha_padrão['VALOR'] = linha_padrão['VALOR'].apply(lambda x: str(x).replace(str(list(linha_padrão['VALOR'])[0]), str(tarifa)))
            linha_padrão['Recebido'] = linha_padrão['Recebido'].apply(lambda x: str(x).replace(str(list(linha_padrão['Recebido'])[0]), str(recebido)))
            linha_padrão['VALOR'] = linha_padrão['VALOR'].astype(np.float64)
            linha_padrão['Movimento'] = linha_padrão['Movimento'].astype(np.int64)
            linha_padrão['Recebido'] = linha_padrão['Recebido'].astype(np.float64)
        else:
            tamanho = len(df_verconcil['VALOR'])
            tamanho = np.zeros(tamanho, dtype=int)
            tamanho = np.where(tamanho == 0, tarifa, tarifa)
            tamanho = pd.DataFrame(data=tamanho, columns=['VALOR'])
            df_verconcil['VALOR'] = tamanho['VALOR'].values
            movimentos = len(df_verconcil.index)
            tarifa = round(df_verconcil['VALOR'].sum(), 2)
            linha_padrão = df_auxiliar.iloc[:1]
            linha_padrão['CLASSIFICACAO'] = linha_padrão['CLASSIFICACAO'].apply(lambda x: str(x).replace(str(list(linha_padrão['CLASSIFICACAO'])[0]), 'Tarifa Python'))
            linha_padrão['DESCRIÇÃO'] = linha_padrão['DESCRIÇÃO'].apply(lambda x: str(x).replace(str(list(linha_padrão['DESCRIÇÃO'])[0]), 'Tarifa Python'))
            linha_padrão['NUM DOCTO'] = linha_padrão['NUM DOCTO'].apply(lambda x: str(x).replace(str(list(linha_padrão['NUM DOCTO'])[0]), ''))
            linha_padrão['C / D'] = linha_padrão['C / D'].apply(lambda x: str(x).replace(str(list(linha_padrão['C / D'])[0]), ''))
            linha_padrão['SALDO'] = linha_padrão['SALDO'].apply(lambda x: str(x).replace(str(list(linha_padrão['SALDO'])[0]), ''))
            linha_padrão['NOME'] = linha_padrão['NOME'].apply(lambda x: str(x).replace(str(list(linha_padrão['NOME'])[0]), ''))
            linha_padrão['CONTA'] = linha_padrão['CONTA'].apply(lambda x: str(x).replace(str(list(linha_padrão['CONTA'])[0]), ''))
            linha_padrão['AGENCIA'] = linha_padrão['AGENCIA'].apply(lambda x: str(x).replace(str(list(linha_padrão['AGENCIA'])[0]), ''))
            linha_padrão['DOCTO CLIENTE'] = linha_padrão['DOCTO CLIENTE'].apply(lambda x: str(x).replace(str(list(linha_padrão['DOCTO CLIENTE'])[0]), ''))
            linha_padrão['GENERICO 1'] = linha_padrão['GENERICO 1'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 1'])[0]), ''))
            linha_padrão['GENERICO 2'] = linha_padrão['GENERICO 2'].apply(lambda x: str(x).replace(str(list(linha_padrão['GENERICO 2'])[0]), ''))
            linha_padrão['Movimento'] = linha_padrão['Movimento'].apply(lambda x: str(x).replace(str(list(linha_padrão['Movimento'])[0]), str(movimentos)))
            linha_padrão['VALOR'] = linha_padrão['VALOR'].apply(lambda x: str(x).replace(str(list(linha_padrão['VALOR'])[0]), str(tarifa)))
            linha_padrão['Recebido'] = linha_padrão['Recebido'].apply(lambda x: str(x).replace(str(list(linha_padrão['Recebido'])[0]), str(recebido)))
            linha_padrão['VALOR'] = linha_padrão['VALOR'].astype(np.float64)
            linha_padrão['Movimento'] = linha_padrão['Movimento'].astype(np.int64)
            linha_padrão['Recebido'] = linha_padrão['Recebido'].astype(np.float64)
        df_custo = pd.concat([linha_padrão,df_custo], ignore_index=True)
        if df_final.empty:
            df_final = df_custo
        else:
            df_final = pd.concat([df_final,df_custo], ignore_index=True)
    df_final.drop('Auxiliar', inplace=True, axis=1)
    df_final['Movimento'] = df_final['Movimento'].astype(np.int64)
    df_final['Recebido'] = df_final['Recebido'].astype(np.float64)
    df_final.to_csv(caminho_parcial+agentes+".csv", encoding='utf-8-sig', index = False)

if __name__ == '__main__':
    inicio = time.perf_counter()
    usuario = os.getcwd().split('\\')[2]
    caminho_inicial = "C:\\Users\\"+usuario+"\\Projetos Python\\Auxiliar\\Cronograma.xlsx"
    caminho_tarifa = "C:\\Users\\"+usuario+"\\Projetos Python\\Auxiliar\\Tarifas BBPix.xlsx"
    caminho_final = "C:\\Users\\"+usuario+"\\Projetos Python\\Análises\\Recebimento PIX\\BBPixFinal.xlsx"
    caminho_parcial = "C:\\Users\\"+usuario+"\\Projetos Python\\temp\\"

    csvs = glob.glob(caminho_parcial+"*csv*")
    if not csvs:
        print('Pasta temp incialmente vazia')
    else:
        print('Deletando arquivos da pasta temp')
        for csv in csvs:
            os.remove(csv)

    df = pd.read_excel(caminho_inicial, sheet_name="Parâmetros Python")
    aba_inicial = list(df['Aba Inicial'])
    aba_auxiliar = list(df['Aba Auxiliar'])
    aba_inicial = aba_inicial[0]
    aba_auxiliar = aba_auxiliar[0]
    df = pd.read_excel(caminho_inicial, sheet_name=aba_inicial)
    print(df)
    agentes = list(df['Agente'])
    caminhos = list(df['Caminho'])
    df = pd.read_excel(caminho_inicial, sheet_name=aba_auxiliar)
    df_tarifas = pd.read_excel(caminho_tarifa, sheet_name="Tabelão Tarifas")
    
    qtd = len(agentes)
    caminho_parcial = [caminho_parcial] * qtd
    df = [df] * qtd
    df_tarifas = [df_tarifas] * qtd
    caminho_tarifa = [caminho_tarifa] * qtd


    print('Executando Loop')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(etl_txt,agentes,caminhos,caminho_parcial,df,caminho_tarifa,df_tarifas)

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

