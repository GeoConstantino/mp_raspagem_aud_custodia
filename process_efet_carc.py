import os
import pandas as pd
import re
import sys
import ipdb

# FLOW recebe new ou old

try:
    FLOW = sys.argv[1]
except:
    FLOW = 'new'


def get_list_files_in():
    # Retorna lista com arquivos na pasta IN

    path = os.getcwd() + "/in/"

    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            
            files.append(os.path.join(r, file))
    return (files)


def get_data_doc(file):

    df_data = pd.read_excel(file, sheet_name=1, header=None)
    first_line = df_data.iloc[0][0]
    regex = r"(\d{2}/\d{2}/\d{4})"
    dt_documento = re.search(regex, first_line)
    return dt_documento[0]


def format_numbers(df):

    for col in df:
        if pd.api.types.is_numeric_dtype(df[col]):
            
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype(int)
    return df


def format_cols_name_base(df):

    cols = ['ID', 'nome', 'localidade', 'regime', 'Erro1', 'cap_original',
            'vagas_inosp', 'cap_atual', 'efetivo_nom', 'baixados', 'acautelado',
            'efetivo_real', 'excesso', 'vagas', 'Erro2', 'Erro3']

    df.columns = cols
    return df


def salva_csv(df,data_documento,filename):

    data = re.sub(r'/','',data_documento)
    df.to_csv("./out/"+filename+data+'.csv', index=False)
    print('Arquivo {} salvo com sucesso.'.format(filename))


def unidade_prisional_processada_new(df_ec, data_documento):

    df_ec.drop(columns=['Erro1','Erro2','Erro3', 'regime'], inplace=True)

    df_ec.dropna(subset=["nome"], inplace=True)

    df_ec['dt_documento'] = data_documento

    df_ec = format_numbers(df_ec)

    df_ec = quebra_nome(df_ec)

    salva_csv(df_ec,data_documento,'unidade_prisional')

    return (df_ec)


def unidade_regime_processada_new(df, data_documento):

    df_p = df[['ID','nome', 'regime']].copy()

    df_p['regime'] = df_p['regime'].str.strip()

    df_p['nome'].fillna(method='ffill', inplace=True)

    df_p['ID'].fillna(method='ffill', inplace=True)

    df_p.dropna(subset=["regime"], inplace=True)

    lista = []

    for i, row in df_p.iterrows():
        
        if row['regime'] in ['Fechado', 'Provisório', 'Semiaberto',
        'Provisório Comum','Provisório Federal', 'Aberto']:
          
            lista.append(row)
            
        elif row['regime'] == "Fech/Sa/Ab/Prov":
            
            line = pd.Series([row[0],row[1],'Fechado'], index=['ID','nome', 'regime'])
            lista.append(line)
            
            line = pd.Series([row[0],row[1],'Semiaberto'], index=['ID','nome', 'regime'])
            lista.append(line)
            
            line = pd.Series([row[0],row[1],'Aberto'], index=['ID','nome', 'regime'])
            lista.append(line)
            
            line = pd.Series([row[0],row[1],'Provisório'], index=['ID','nome', 'regime'])
            lista.append(line)
            
        elif row['regime'] == "Med.de Seg.":
            
            line = pd.Series([row[0],row[1],'Medida de Segurança'], index=['ID','nome', 'regime'])
            lista.append(line)
    
    md_regime = pd.DataFrame(lista)

    md_regime['dt_documento'] = data_documento

    md_regime = format_numbers(md_regime)

    md_regime = quebra_nome(md_regime)

    salva_csv(md_regime,data_documento,'unidade_regime')

    return (md_regime)


def format_cols_name_fac(df):

    f_cols = ['ID', 'nome', 'localidade', 'regime', 'cap_atual', 'efetivo_real','grupo','Erro7', 'Erro8', 'Erro9',
       'Erro1', 'Erro2', 'Erro3']

    df.columns = f_cols

    df.drop(columns=['localidade','regime','cap_atual','efetivo_real','Erro7', 'Erro8', 'Erro9',
       'Erro1', 'Erro2', 'Erro3'], inplace=True)

    df.dropna(subset=["grupo"], inplace=True)

    return df


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


def unidade_faccoes_processada_new(file, data_documento):

    df_fac = pd.read_excel(file, sheet_name=2, header=2)

    df_fac = format_cols_name_fac(df_fac)
    
    lista_grupo = ['CV','TC','NEUTRO','NÍVEL SUPERIOR','FEDERAL','ING. MAS','FEM','IDOSO','CADEI','NEUTRO PRIMARIOS',
'POL. CIVIL','ISAP ATIVA','VPL TEM','TEM','ADA','EX POLICIAL','ING. FEDERAL','FEDERAIS'
,'GERAL','MILÍCIA','PRISÃO ESPECIAL','PA']

    flista = []

    for i, linha in df_fac.iterrows():

        for grupo in lista_grupo:

            res = busca_grupo(grupo, linha)
            if res is not None:

                flista.append(res)

    df_grupo = pd.DataFrame(flista)

    df_grupo['dt_documento'] = data_documento

    df_grupo = format_numbers(df_grupo)

    df_grupo.drop_duplicates(inplace=True)

    df_grupo = quebra_nome(df_grupo)

    salva_csv(df_grupo,data_documento,'unidade_grupo')

    return (df_grupo)


def quebra_nome(df):
    
    ser = df['nome']

    sigla = []

    for i in ser:

        regex = "([A-Z]{3,7})(\s|$)"
            
        try:
            z = re.search(regex,i)[0]
        except:
            if i == "Instituto Penal Oscar Stevenson":
                z = 'SEAPOS'

        sigla.append(z.strip())
        #print(z)

    df['sigla'] = sigla

    return df
    

def put_zeros(df):

    df['cap_original'].fillna(0, inplace=True)
    df['vagas_inosp'].fillna(0, inplace=True)
    df['cap_atual'].fillna(0, inplace=True)
    df['efetivo_nom'].fillna(0, inplace=True)
    df['baixados'].fillna(0, inplace=True)
    df['acautelado'].fillna(0, inplace=True)
    df['efetivo_real'].fillna(0, inplace=True)
    df['excesso'].fillna(0, inplace=True)
    df['vagas'].fillna(0, inplace=True)

    return df

def unidade_prisional_processada_old(file, data_documento):

    plan_num = [0,1,2]

    cols = ['ID', 'nome', 'localidade', 'regime', 'cap_original',
            'vagas_inosp', 'cap_atual', 'efetivo_nom', 'baixados', 'acautelado',
            'efetivo_real', 'excesso', 'vagas']

    df_ec = pd.DataFrame()

    for i in plan_num:

        if i == 2:
            
            df = pd.read_excel(file, sheet_name=i, header=4)
        
        else:
            
            df = pd.read_excel(file, sheet_name=i)

      
        if i == 1:

            if set(['Unnamed: 4']).issubset(df.columns):
                df.drop(columns=['Unnamed: 4'], inplace=True)

            if set(['Unnamed: 14']).issubset(df.columns):
                df.drop(columns=['Unnamed: 14'], inplace=True)

        
        df.columns = cols

        df = df.iloc[3:]
        
        df = put_zeros(df)

        df.dropna(subset=["regime"], inplace=True)

        df = df[df['cap_original'] != 0]

        df['nome'].fillna(method='ffill', inplace=True)

        df['ID'].fillna(method='ffill', inplace=True)

        df['localidade'] = df['localidade'].str.strip()

        df['localidade'].fillna(method='ffill', inplace=True)

        df = df.infer_objects()

        df = df.groupby(["ID","nome","localidade"], as_index=False).sum()

        
        df_ec = df_ec.append(df, sort=False)

    df_ec['dt_documento'] = data_documento
    
    df_ec = quebra_nome(df_ec)

    salva_csv(df_ec,data_documento,'unidade_prisional')

    return df_ec
    

def unidade_regime_processada_old(file, data_documento):

    df_all = pd.DataFrame()

    plan_num = [4,5,6]

    cols = ['ID', 'nome', 'localidade', 'regime', 'cap_atual',
        'efetivo_real', 'excesso', 'perc_excesso']

    df_un = pd.DataFrame()

    for i in plan_num:

        df = pd.read_excel(file, sheet_name=i, header=0)

        if i > 4:

            if set(['Unnamed: 8']).issubset(df.columns):
                df.drop(columns=['Unnamed: 8'], inplace=True)

            if set(['Unnamed: 9']).issubset(df.columns):
                df.drop(columns=['Unnamed: 9'], inplace=True)

            if set(['Unnamed: 10']).issubset(df.columns):
                df.drop(columns=['Unnamed: 10'], inplace=True)

            if set(['Unnamed: 11']).issubset(df.columns):
                df.drop(columns=['Unnamed: 11'], inplace=True)

            if set(['Unnamed: 12']).issubset(df.columns):
                df.drop(columns=['Unnamed: 12'], inplace=True)
                
        df.columns = cols

        df = df.iloc[3:]
            
        df_e = df[['ID','nome','regime']].copy()
        
        df_all = df_all.append(df_e, sort=False)

    df_all.dropna(subset=['regime'], inplace=True)
    
    df_all['ID'].fillna(method='ffill', inplace=True)
    
    df_all['nome'].fillna(method='ffill', inplace=True)

    df_all['regime'] = df_all['regime'].str.strip()
    
    df_all['nome'] = df_all['nome'].str.strip()
            

    list = []

    for i, row in df_all.iterrows():
        
        if row['regime'] in ['Fechado','Fem.-Fechado']:
            line = pd.Series([row[0],row[1],'Fechado'], index=['ID','nome', 'regime'])
        
        elif row['regime'] in ['Provisório','Fem.- Provisório']:
            line = pd.Series([row[0],row[1],'Provisório'], index=['ID','nome', 'regime'])
        
        elif row['regime'] in ['Fem.-Semiaberto','Semiaberto']:
            line = pd.Series([row[0],row[1],'Semiaberto'], index=['ID','nome', 'regime'])
        
        elif row['regime'] in ['Fem.Aberto','Fem.-Aberto','Aberto']:
            line = pd.Series([row[0],row[1],'Aberto'], index=['ID','nome', 'regime'])
        
        elif row['regime'] in ['Fem.- Provisório Comum']:
            line = pd.Series([row[0],row[1],'Provisório Comum'], index=['ID','nome', 'regime'])
            
        elif row['regime'] in ['Fem -  Provisório Federal']:
            line = pd.Series([row[0],row[1],'Provisório Federal'], index=['ID','nome', 'regime'])
            
        elif row['regime'] in ['Outros']:
            line = pd.Series([row[0],row[1],'Outros'], index=['ID','nome', 'regime'])
        
        
        
        if line.isnull().all() ==  False:
            list.append(line)
    
    df_regime = pd.DataFrame(list)

    df_regime.drop_duplicates()

    df_regime['dt_documento'] = data_documento

   

    df_regime = quebra_nome(df_regime)

    salva_csv(df_regime,data_documento,'unidade_regime')

    return df_regime


def unidade_faccoes_processada_old():



########### MAIN ###########
if __name__ == '__main__':

    # Listar arquivos a serem lidos
    
    for file in get_list_files_in():
        
        print (file)

        data_documento = get_data_doc(file)
        
        if FLOW == 'new':
            
            df = pd.read_excel(file, sheet_name=0, header=8) # 1 == Efetivo Simplificado
            
            df = format_cols_name_base(df)

            unidade_prisional_processada_new(df.copy(),data_documento)

            unidade_regime_processada_new(df.copy(),data_documento)

            unidade_faccoes_processada_new(file,data_documento)


        elif FLOW == 'old':
            
            unidade_prisional_processada_old(file, data_documento)
            
            unidade_regime_processada_old(file, data_documento)

            unidade_faccoes_processada_old(file, data_documento)




            pass;

        