import pandas as pd
import msoffcrypto
import io
import os
import numpy as np
import time
import concurrent.futures
import glob
import shutil

def etl_txt(agentes,arquivos,caminhos,caminho_inicial,caminho_final,caminho_erro,aba_auxiliar):
    print('Iniciando '+ agentes)
    if os.path.isfile(caminhos):
        df = pd.read_excel(caminho_inicial, sheet_name=aba_auxiliar)
        df1 = df[df['Agente'] == agentes]
        abas = list(df1['Nome da Aba'])
        df2 = pd.DataFrame()
        abertura_excel = io.BytesIO()
        with open(caminhos, 'rb') as abertura:
            excel = msoffcrypto.OfficeFile(abertura)
            excel.load_key(('apc2016'))
            excel.decrypt(abertura_excel)
        abas_conferencia = pd.ExcelFile(abertura_excel)
        abas_conferencia = abas_conferencia.sheet_names
        abas_conferencia = list(filter(lambda x: x.find('_') != -1, abas_conferencia))
        abas_conferencia = list(filter(lambda x: x.find(' ') == -1, abas_conferencia))
        abas_conferencia = list(filter(lambda x: x.find('OP_') == -1, abas_conferencia))
        abas = sorted(abas)
        abas_conferencia = sorted(abas_conferencia)
        df4 = pd.DataFrame()
        if abas == abas_conferencia:
            df3 = pd.read_excel(abertura_excel, sheet_name = abas_conferencia, header = 5, usecols = "A:AS", na_filter=False)
            del abertura_excel
            for aba in abas_conferencia:
                df_auxiliar = df3[aba]
                df_auxiliar2 = df1[df1['Nome da Aba'] == aba]
                responsaveis = list(df_auxiliar2['Responsável'])
                grupos = list(df_auxiliar2['País / Grupo'])
                bancos = list(df_auxiliar2['Banco'])
                agencias = list(df_auxiliar2['Agência'])
                contas = list(df_auxiliar2['Conta'])
                responsavel = responsaveis[0]
                grupo = grupos[0]
                banco = bancos[0]
                agencia = agencias[0]
                conta = contas [0]
                df_auxiliar.insert(0,'Conta', value = conta)
                df_auxiliar.insert(0,'Agência', value = agencia)
                df_auxiliar.insert(0,'Banco', value = banco)
                df_auxiliar.insert(0,'Agente', value = agentes)
                df_auxiliar.insert(0,'País / Grupo', value = grupo)
                df_auxiliar.insert(0,'Status / Aba', value = aba)
                df_auxiliar.insert(0,'Responsável', value = responsavel)
                if df2.empty:
                    df2 = df_auxiliar
                else:
                    df2 = pd.concat([df2,df_auxiliar], ignore_index=True)
            df2.to_csv(caminho_final+arquivos+".txt", encoding='utf-8-sig', index = False)
            print('Base de '+ agentes+' salva')
        else:
            print('As abas não batem com a base, salvando arquivo e lendo próxima base')
            df5 = {'Agente': agentes,
                'ListaCronograma': abas,
                'ListaBase': abas_conferencia},
            df5 = pd.DataFrame(df5)
            if df4.empty:
                df4 = df5
            else:
                df4 = pd.concat([df4,df5])
            df4.to_csv(caminho_erro+agentes+'.csv', index = False)
            del abertura_excel          
    else:
        print('A base não foi encontrada na pasta, lendo próxima base')

if __name__ == '__main__':
    inicio = time.perf_counter()
    usuario = os.getcwd().split('\\')[2]
    caminho_inicial = "C:\\Users\\"+usuario+"\\Projetos Python\\Auxiliar\\Cronograma.xlsx"
    caminho_final = "C:\\Users\\"+usuario+"\\Projetos Python\\Custos Consolidados\\"
    caminho_erro = "C:\\Users\\"+usuario+"\\Projetos Python\\temp\\"
    caminho_erro_final = "C:\\Users\\"+usuario+"\\Projetos Python\\Auxiliar\\ErrosCronograma.xlsx"

    df = pd.read_excel(caminho_inicial, sheet_name="Parâmetros Python")
    mes = list(df['Mês'])
    ano = list(df['Ano'])
    mes = int(mes[0])
    ano = str(int(ano[0]))
    if mes < 10:
        mes = '0' + str(mes)
    else:
        mes = str(mes)
    aba_auxiliar = 'Abas'

    caminho_blenet = "C:\\Users\\"+usuario+"\\"+ano+"\\"+mes+"."+ano+"\\BRASIL B\\"
    caminho_t1 = "C:\\Users\\"+usuario+"\\"+ano+"\\"+mes+"."+ano+"\\BRASIL B1\\"
    caminho_t2 = "C:\\Users\\"+usuario+"\\"+ano+"\\"+mes+"."+ano+"\\BRASIL B2\\"

    csvs = glob.glob(caminho_erro+"*csv*")
    if not csvs:
        print('Pasta temp incialmente vazia')
    else:
        print('Deletando arquivos da pasta temp')
        for csv in csvs:
            os.remove(csv)

    bases = [caminho_blenet, caminho_t1, caminho_t2]
    caminhos = list()
    for base in bases:
        base = glob.glob(base+"*Base Extratos*")
        for base in base:
            caminhos.append(base)

    print('Você deseja rodar todas as bases ou alguma específica?')
    print('1 - Todas')
    print('2 - Quero escolher')
    resposta = input('Digite: ')

    while int(resposta) != 1 and int(resposta) != 2:
        print('Opção é não válida, escolha novamente')
        print('1 - Todas')
        print('2 - Quero escolher')
        resposta = input('Digite: ')
    
    if int(resposta) == 1:
        pass
    else:
        print('Digite os nomes dos agentes separado por virgulas')
        resposta = input('Digite: ')
        agentes = resposta.split(',')
        caminhos_auxiliar = caminhos
        caminhos = list()
        for agente in agentes:
            caminho = caminhos_auxiliar
            caminho = list(filter(lambda x: x.find(agente) != -1, caminho))
            caminho = caminho[0]
            caminhos.append(caminho)
    
    #BR.Dalnet,BR.Dtsosa,BR.Geral,BR.Renovapay,BR.Yugen

    agentes = list()
    for caminho in caminhos:
        agente = caminho.split('Base Extratos - ')[1]
        agente = agente[:-5]
        agentes.append(agente)
    
    arquivos = list()
    for caminho in caminhos:
        arquivo = caminho.split(mes+'.'+ano+' - ')[1]
        arquivo = arquivo[:-5]
        arquivo = mes+'.'+ano+' - '+arquivo
        arquivos.append(arquivo)

    qtd = len(agentes)
    caminho_inicial = [caminho_inicial] * qtd
    caminho_final = [caminho_final] * qtd
    caminho_erro = [caminho_erro] * qtd
    aba_auxiliar = [aba_auxiliar] * qtd

    print('Executando Loop')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(etl_txt,agentes,arquivos,caminhos,caminho_inicial,caminho_final,caminho_erro,aba_auxiliar)

    csvs = glob.glob(caminho_erro[0]+"*csv*")
    df = pd.DataFrame()
    if not csvs:
        print('Pasta temp vazia, não teve erro nas bases')
    else:
        for csv in csvs:
            df1 = pd.read_csv(csv, encoding='utf-8-sig')
            if df.empty:
                df = df1
            else:
                df = pd.concat([df,df1])
        print('Salvando aqruivo de erros')
        df.to_excel(caminho_erro_final, index = False)

    fim = time.perf_counter()
    print((time.strftime("%H:%M:%S", time.gmtime(fim-inicio))))
