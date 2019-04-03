import pandas as pd

file = "./OLD/2016.08.02.xls"

planilha_numero = 0

df = pd.read_excel(file, sheet_name=planilha_numero)

cols = ['ID', 'nome', 'localidade', 'regime', 'cap_original',
            'vagas_inosp', 'cap_atual', 'efetivo_nom', 'baixados', 'acautelado',
            'efetivo_real', 'excesso', 'vagas']

df.columns = cols

df = df.iloc[3:]
#df.drop(columns=['regime'], inplace=True)

#Put Zeros on Extended Rows
df['cap_original'].fillna(0, inplace=True)
df['vagas_inosp'].fillna(0, inplace=True)
df['cap_atual'].fillna(0, inplace=True)
df['efetivo_nom'].fillna(0, inplace=True)
df['baixados'].fillna(0, inplace=True)
df['acautelado'].fillna(0, inplace=True)
df['efetivo_real'].fillna(0, inplace=True)
df['excesso'].fillna(0, inplace=True)
df['vagas'].fillna(0, inplace=True)

df.dropna(subset=["regime"], inplace=True)

df = df[df['efetivo_nom'] != 0]

#df['nome'].fillna(method='ffill', inplace=True)

#df['ID'].fillna(method='ffill', inplace=True)

#df['localidade'].fillna(method='ffill', inplace=True)

#df = df.infer_objects()

#df = df.groupby(["ID","nome"], as_index=False).sum()