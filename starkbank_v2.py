import pandas as pd
import numpy as np
import glob

def open_csv(filepath):
    filename = filepath.split('\\')[1].replace('.csv','')
    df = pd.read_csv(filepath)
    df['data'] = df['created'].str[:10]
    df["num_docto"] = np.where(df['tags'].str.split(',').str.len()>1, df['tags'].str.split(',').str.get(0), df['tags'])
    df["moeda"] = "BRL"
    df["valor"] = df['amount']
    df["CD"] = np.where(df['amount']>0, 'C', 'D')
    df["saldo"] = df['balance']
    df["nome"] = (df['description']).str.split('(').str.get(0)
    df["conta"] = ""
    df["agencia"] = ""
    df["docto_cliente"] = (df['description']).str.split('(').str.get(1).str[:14]
    df["generico_1"] = df['id']
    df["generico_2"] = df['source']
    df["generico_3"] = np.where(df.groupby('num_docto')['num_docto'].transform('count')>1,'Provavel DOC/TED Dev','')
    df2 = df[['data','num_docto','moeda','valor','CD','saldo','nome','conta','agencia','docto_cliente','generico_1','generico_2','generico_3']]
    df2['descricao'] = np.where(df['nome'].str.upper().str.contains('BOLETO'),'PRINCIPAL - BOLETO',np.where(df['CD']=='C','PRINCIPAL - CREDITO',np.where(df['CD']=='D','PRINCIPAL - DEBITO','PRINCIPAL - SEM CLASS.')))
    df['descricao'] = df2['descricao']

    principal = df[['data','descricao','num_docto','moeda','valor','CD','saldo','nome','conta','agencia','docto_cliente','generico_1','generico_2','generico_3','created']]
    principal['saldo'] = ''

    tarifa = df[['data','num_docto','moeda','valor','CD','saldo','nome','conta','agencia','docto_cliente','generico_1','generico_2','generico_3','created']]
    tarifa['descricao'] = np.where(df['nome'].str.upper().str.contains('BOLETO'),'TARIFA - BOLETO',np.where(df['CD']=='C','TARIFA - CREDITO',np.where(df['CD']=='D','TARIFA - DEBITO','TARIFA - SEM CLASS.')))
    tarifa['nome'] = ''
    tarifa['docto_cliente'] = ''
    tarifa['valor'] = df['fee']

    final = pd.concat([principal, tarifa])
    final['index'] = final.index
    final = (final.sort_values(by=['index','descricao']))

    final = final[['data','descricao','num_docto','moeda','valor','CD','saldo','nome','conta','agencia','docto_cliente','generico_1','generico_2','generico_3']]

    print(final)


    final.to_csv("Outputs\\"+filename+".csv",index = False)



files = glob.glob('Files/*.csv')
for file in files:
    print(file)
    open_csv(file)

