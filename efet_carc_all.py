import re
import sys
import os

try:
    import pandas as pd
except ImportError:
    print("Instalando módulo necessário:'pandas'\n")
    os.system('python -m pip install pandas')
    os.system('python -m pip install xlrd')
    import pandas as pd

## leitura do arquivo
file_name = ""
try:
    file_name = sys.argv[1]

except:
    pass

if os.path.exists(file_name):
    arquivo = file_name
    print('Lendo arquivo:{}'.format(arquivo))
else:
    
    if file_name == "":
        print('Não foi passado um arquivo.')
        if os.path.exists('efetivo_carcerario.xlsx'):
            arquivo = 'efetivo_carcerario.xlsx'
            print('Lendo arquivo padrão {}'.format(arquivo))
        else:
            print('Arquivo padrão: "efetivo_carcerario.xlsx" inexistente.')
            sys.exit()
    else:
        print('Arquivo especificado não existe.')
        sys.exit()



df_ec = pd.read_excel(arquivo, header=7, sheet_name="Efetivo Completo")

df_fac = pd.read_excel(arquivo, sheet_name="Efetivo Com Facções", header=2)


def get_data_documento():

    df_data = pd.read_excel(arquivo, sheet_name="Efetivo Simplificado", header=None)
    first_line = df_data.iloc[0][0]
    regex = r"(\d{2}/\d{2}/\d{4})"
    dt_documento = re.search(regex, first_line)
    return dt_documento[0]


DATA_DOCUMENTO = get_data_documento()

## preparação do csv base com os dados das UPs

cols = ['ID', 'nome', 'localidade', 'regime', 'Unnamed: 4', 'cap_original',
       'vagas_inosp', 'cap_atual', 'efetivo_nom', 'baixados', 'acautelado',
       'efetivo_real', 'excesso', 'vagas', 'Unnamed: 14', 'Unnamed: 15']

df_ec.columns = cols

df = df_ec[['ID','nome', 'regime']].copy()

df_ec.drop(columns=['Unnamed: 4','Unnamed: 14','Unnamed: 15', 'regime'], inplace=True)

df_ec.dropna(subset=["nome"], inplace=True)


df_ec["dt_documento"] = DATA_DOCUMENTO

## fim bloco data

# percorre o DF e transforma as colunas de numeros em Inteiros


# Normaliza os Números 
def normNum(df_ec):

    for col in df_ec:
        if pd.api.types.is_numeric_dtype(df_ec[col]):
            
            df_ec[col] = df_ec[col].fillna(0)
            df_ec[col] = df_ec[col].astype(int)
    return df_ec

#import ipdb;ipdb.set_trace()
df_ec = normNum(df_ec)


## grava o primeiro arquivo no disco
df_ec.to_csv("unidade_prisional.csv",index=False)
print('Arquivo "unidade_prisional.csv" criado')
## fim  bloco preparacao csv base

df_cols = ['ID','nome','regime_tipo']

df.columns = df_cols

df['regime_tipo'] = df['regime_tipo'].str.strip()

df['nome'].fillna(method='ffill', inplace=True)

df['ID'].fillna(method='ffill', inplace=True)

df.dropna(subset=["regime_tipo"], inplace=True)

lista = []

for i, row in df.iterrows():
    
    if row['regime_tipo'] in ['Fechado', 'Provisório', 'Semiaberto',
       'Provisório Comum','Provisório Federal', 'Aberto']:
        #append on df
        #print('clear')
        lista.append(row)
        
    elif row['regime_tipo'] == "Fech/Sa/Ab/Prov":
        import ipdb; ipdb.set_trace()
        line = pd.Series([row[0],row[1],'Fechado'], index=['ID','nome', 'regime_tipo'])
        lista.append(line)
        
        line = pd.Series([row[0],row[1],'Semiaberto'], index=['ID','nome', 'regime_tipo'])
        lista.append(line)
        
        line = pd.Series([row[0],row[1],'Aberto'], index=['ID','nome', 'regime_tipo'])
        lista.append(line)
        
        line = pd.Series([row[0],row[1],'Provisório'], index=['ID','nome', 'regime_tipo'])
        lista.append(line)
        
    elif row['regime_tipo'] == "Med.de Seg.":
        
        line = pd.Series([row[0],row[1],'Medida de Segurança'], index=['ID','nome', 'regime_tipo'])
        lista.append(line)

md_regime = pd.DataFrame(lista)

md_regime["dt_documento"] = DATA_DOCUMENTO

md_regime = normNum(md_regime)

md_regime.to_csv('unidade_regime.csv',index=None)
print('Arquivo "unidade_regime.csv" criado')


### bloco de faccoes

f_cols = ['ID', 'nome', 'localidade', 'regime', 'cap_atual', 'efetivo_real','grupo','Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9',
       'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12']

df_fac.columns = f_cols

df_fac.drop(columns=['localidade','regime','cap_atual','efetivo_real','Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9',
       'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12'], inplace=True)


df_fac.dropna(subset=["grupo"], inplace=True)

flista = []


def busca_grupo(grupo, linha):

    if grupo.lower() in linha['grupo'].lower():
        
        if (grupo.lower() != 'neutro') and (grupo.lower() != 'neutro primários') \
        and (grupo.lower() != 'federal') and (grupo.lower() != 'ing. federal') \
        and (grupo.lower() != 'vlp tem') and (grupo.lower() != 'tem'):

            cel = pd.Series([linha[0],linha[1], grupo], index=['ID','nome','grupo'])
            return cel

        else:
            
            if "NEUTRO PRIMÁRIOS".lower() in linha['grupo'].lower():
                cel = pd.Series([linha[0],linha[1], "NEUTRO PRIMÁRIOS"], index=['ID','nome','grupo'])
                return cel

            elif "NEUTRO".lower() in linha['grupo'].lower():
                cel = pd.Series([linha[0],linha[1], "NEUTRO"], index=['ID','nome','grupo'])
                return cel

            elif "ING. FEDERAL".lower() in linha['grupo'].lower():
                cel = pd.Series([linha[0],linha[1], "ING. FEDERAL"], index=['ID','nome','grupo'])
                return cel

            elif "FEDERAL".lower() in linha['grupo'].lower():
                cel = pd.Series([linha[0],linha[1], "FEDERAL"], index=['ID','nome','grupo'])
                return cel

            elif "VLP TEM".lower() in linha['grupo'].lower():
                cel = pd.Series([linha[0],linha[1], "VLP TEM"], index=['ID','nome','grupo'])
                return cel

            elif "TEM".lower() in linha['grupo'].lower():
                cel = pd.Series([linha[0],linha[1], "TEM"], index=['ID','nome','grupo'])
                return cel

lista_grupo = ['CV','TC','NEUTRO','NÍVEL SUPERIOR','FEDERAL','ING. MAS','FEM','IDOSO','CADEI','NEUTRO PRIMARIOS',
'POL. CIVIL','ISAP ATIVA','VPL TEM','TEM','ADA','EX POLICIAL','ING. FEDERAL','FEDERAIS'
,'GERAL','MILÍCIA','PRISÃO ESPECIAL','PA']

# Roda o DF e verifica

for i, linha in df_fac.iterrows():

    for grupo in lista_grupo:

        res = busca_grupo(grupo, linha)
        if res is not None:

            flista.append(res)

md_grupo = pd.DataFrame(flista)

md_grupo["dt_documento"] = DATA_DOCUMENTO

md_grupo = normNum(md_grupo)

md_grupo.drop_duplicates(inplace=True)

md_grupo.to_csv('unidade_grupo.csv',index=None)

print('Arquivo "unidade_grupo.csv" criado')