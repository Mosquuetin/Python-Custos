import pandas as pd
import os
import numpy as np
import time
import concurrent.futures
import glob
import shutil

usuario = os.getcwd().split('\\')[2]
caminho_inicial = "C:\\Users\\"+usuario+"\\Projetos Python\\Auxiliar\\Cronograma.xlsx"
caminho_parcial = "C:\\Users\\"+usuario+"\\Projetos Python\\Análises\\Análises Gerais\\"

def etl_txt(arquivos,caminho_parcial):
    agente = arquivos.split('\\')[8]
    agente = agente[:-4]
    agente = agente.split('Extratos - ')[1]
    print('Iniciando '+ agente)
    df3 = pd.DataFrame()
    df5 = pd.DataFrame()
    df9 = pd.DataFrame()
    df13 = pd.DataFrame()
    df18 = pd.DataFrame()
    df23 = pd.DataFrame()
    df25 = pd.DataFrame()
    df27 = pd.DataFrame()
    df1 = pd.read_csv(arquivos, encoding='utf-8-sig', low_memory=False)
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
    df20 = df1
    df20 = df20[df20['CLASSIFICACAO'].str.contains('cat. Investimento', na = False)]
    if df5.empty:
        df5 = df20
    else:
        df5 = pd.concat([df5,df20])
    #Fim do Relevantes Fora, Início Classificação
    df8 = df1
    df8 = df8[df8['CLASSIFICACAO'].str.contains("Boletos \[cat.", na = False)]
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
    df17 = df17[df17['CLASSIFICACAO'].str.contains('TEF \[cat. Entrada', na = False)]
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
    df22 = df1
    df22['DATA'] = df22['DATA'].apply(lambda x: str(x).replace('nan', ''))
    df22['CLASSIFICACAO'] = df22['CLASSIFICACAO'].apply(lambda x: str(x).replace('nan', ''))
    df22 = df22[df22['CLASSIFICACAO'] == '']
    df22 = df22[df22['DATA'] != '']
    if df9.empty:
        df9 = df22
    else:
        df9 = pd.concat([df9,df22])
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
    df21 = df1
    df21 = df21[df21['CLASSIFICACAO'].str.contains('SISPAG', na = False)]
    df21 = df21[df21['Status / Aba'].str.contains('ITA', na = False)]
    if df13.empty:
        df13 = df21
    else:
        df13 = pd.concat([df13,df21])
        #Fim do Transf e Expat, Inicio Selenium
    df19 = df1
    df19 = df19[df19['CLASSIFICACAO'].str.contains('Selenium', na = False)]
    if df18.empty:
        df18 = df19
    else:
        df18 = pd.concat([df18,df19])
        #Fim do Selenium Inicio BR.Contabilidade
    df24 = df1
    df24 = df24[df24['CLASSIFICACAO'].str.contains('cat. Custos', na = False)]
    if df23.empty:
        df23 = df24
    else:
        df23 = pd.concat([df23,df24])
        #Fim do BR.Contabilidade Inicio Proc (Maike)
    df26 = df1
    df26 = df26[df26['CLASSIFICACAO'].str.contains('Proc', na = False)]
    if df25.empty:
        df25 = df26
    else:
        df25 = pd.concat([df25,df26])
        #Fim do BR.Proc Inicio Perdas
    df28 = df1
    df28 = df28[df28['CLASSIFICACAO'].str.contains('Perdas', na = False)]
    if df27.empty:
        df27 = df28
    else:
        df27 = pd.concat([df27,df28])
    print('Salvando arquivo de '+agente)
    #df3['VALOR'] = df3['VALOR'].astype(np.float64)
    #df3['DATA'] = df3['DATA'].astype(np.int64)
    df3.to_csv(caminho_parcial+"BR.PgtoADM"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)
    #df5['VALOR'] = df5['VALOR'].astype(np.float64)
    #df5['DATA'] = df5['DATA'].astype(np.int64)
    df5.to_csv(caminho_parcial+"BR.RelevantesFora"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)
    #df9['VALOR'] = df9['VALOR'].astype(np.float64)
    #df9['DATA'] = df9['DATA'].astype(np.int64)
    df9.to_csv(caminho_parcial+"BR.Classificacao"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)
    #df13['VALOR'] = df13['VALOR'].astype(np.float64)
    #df13['DATA'] = df13['DATA'].astype(np.int64)
    df13.to_csv(caminho_parcial+"BR.TransfExpat"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)
    #df18['VALOR'] = df18['VALOR'].astype(np.float64)
    #df18['DATA'] = df18['DATA'].astype(np.int64)
    df18.to_csv(caminho_parcial+"BR.Selenium"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)
    #df23['VALOR'] = df23['VALOR'].astype(np.float64)
    #df23['DATA'] = df23['DATA'].astype(np.float64)
    #df23['DATA'] = df23['DATA'].astype(np.int64)
    df23.to_csv(caminho_parcial+"BR.Contabilidade"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)
    #df25['VALOR'] = df25['VALOR'].astype(np.float64)
    #df25['DATA'] = df25['DATA'].astype(np.int64)
    df25.to_csv(caminho_parcial+"BR.Proc"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)
    #df27['VALOR'] = df27['VALOR'].astype(np.float64)
    #df27['DATA'] = df27['DATA'].astype(np.int64)
    df27.to_csv(caminho_parcial+"BR.Perdas"+ " - " + agente+'.csv', encoding='utf-8-sig', index = False)

if __name__ == '__main__':
    inicio = time.perf_counter()
    usuario = os.getcwd().split('\\')[2]
    caminho_final = "C:\\Users\\"+usuario+"\\Projetos Python\\Análises\\Análises Gerais\\"
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
    

    arquivos = ['BR.PgtoADM','BR.RelevantesFora','BR.Classificacao','BR.TransfExpat','BR.Selenium','BR.Contabilidade','BR.Proc','BR.Perdas']

    for arquivo in arquivos:
        csvs = glob.glob(caminho_parcial[0]+"*"+arquivo+"*")
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
            print('Salvando arquivo final '+arquivo)
            df.to_excel(caminho_final+arquivo+'.xlsx', index = False)

    fim = time.perf_counter()
    print((time.strftime("%H:%M:%S", time.gmtime(fim-inicio))))