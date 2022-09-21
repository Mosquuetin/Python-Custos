import pandas as pd
import msoffcrypto
import io
import os

#caminho_inicial = os.getcwd()+"\Auxiliar\Cronograma.xlsx"
#caminho_final = os.getcwd()+"\Auxiliar\Resumos.csv"
caminho_inicial = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Auxiliar\Cronograma.xlsx"
caminho_final = os.getcwd()[ : -10]+r"\Directa24 Dropbox\Auditoria\Gerencial (temporaria)\2022\Projetos Python\Análises\Análises Gerais\Resumos.csv"

df = pd.read_excel(caminho_inicial, sheet_name="Parâmetros Python")
aba_inicial = list(df['Aba Inicial'])
aba_inicial = aba_inicial[0]
df = pd.read_excel(caminho_inicial, sheet_name=aba_inicial)
print('Arquivo aberto')
print(df)
agentes = list(df['Agente'])
arquivos = list(df['Arquivo'])
caminhos = list(df['Caminho'])
print(df)
print('Executando Loop')
i = 0
df2 = pd.DataFrame()
for agente in agentes:
    print('Iniciando '+ agente)
    caminho = caminhos[i]
    arquivo = arquivos[i]
    abertura_excel = io.BytesIO()
    with open(caminho, 'rb') as abertura:
        excel = msoffcrypto.OfficeFile(abertura)
        excel.load_key(('apc2016'))
        excel.decrypt(abertura_excel)
    df3 = pd.read_excel(abertura_excel, sheet_name = "Resumo", na_filter=False)
    print(df3)
    del abertura_excel
    if df2.empty:
        df2 = df3
    else:
        df2 = pd.concat([df2,df3])
    i = i + 1               
print('Salvando Arquivo')
df2.to_csv(caminho_final, index = False)