import pandas as pd
from datetime import datetime

def ler_arquivos():
    try:
        df_csv = pd.read_csv('Questao02/usuarios.csv')
    except FileNotFoundError:
        df_csv = pd.DataFrame()

    try:
        df_json = pd.read_json('Questao02/usuarios.json')
    except FileNotFoundError:
        df_json = pd.DataFrame()

    try:
        df_excel = pd.read_excel('Questao02/usuarios.xlsx')
    except FileNotFoundError:
        df_excel = pd.DataFrame()

    return df_csv, df_json, df_excel

def padronizar_data(data):
    try:
        if len(data) == 8 and '/' in data:
            partes = data.split('/')
            ano = partes[0]
            if int(ano) > 20:
                ano = '19' + ano
            else:
                ano = '20' + ano
            data_formatada = f"{ano}-{partes[1]}-{partes[2]}"
            datetime.strptime(data_formatada, '%Y-%m-%d')
            return data_formatada
        else:
            return data
    except (ValueError, IndexError):
        return None

def limpar_dados(df):
    df = df.ffill()

    df['data_nascimento'] = df['data_nascimento'].apply(padronizar_data)
    df['data_nascimento'] = pd.to_datetime(df['data_nascimento'], errors='coerce')
    df['data_nascimento'] = df['data_nascimento'].dt.strftime('%Y/%m/%d')

    df = df.dropna()

    return df

def consolidar_dados(df_csv, df_json, df_excel):
    df_final = pd.concat([df_csv, df_json, df_excel], ignore_index=True)
    df_final.reset_index(drop=True, inplace=True)

    # Remover a coluna de ID para evitar conflitos
    if 'id' in df_final.columns:
        df_final.drop(columns=['id'], inplace=True)

    # Realizar limpeza preliminar dos dados
    df_final = limpar_dados(df_final)

    # Remover duplicatas com base nas colunas relevantes
    colunas_relevantes = ['nome_completo', 'data_nascimento', 'email', 'cidade', 'estado', 'consoles', 'jogos_preferidos']
    df_final = df_final.drop_duplicates(subset=colunas_relevantes)

    # Gerar um novo ID único para cada usuário
    df_final['id'] = range(1, len(df_final) + 1)

    # Reorganizar as colunas no DataFrame final
    nova_ordem = ['id'] + [col for col in df_final.columns if col != 'id']
    df_final = df_final.reindex(columns=nova_ordem)

    return df_final

def exportar_dados(df, nome_arquivo):
    try:
        df.to_excel(nome_arquivo, index=False, engine='openpyxl')
        print(f'Dados exportados com sucesso para {nome_arquivo}')
    except Exception as e:
        print(f'Ocorreu um erro ao exportar para Excel: {str(e)}')

def main():
    df_csv, df_json, df_excel = ler_arquivos()
    df_consolidado = consolidar_dados(df_csv, df_json, df_excel)
    exportar_dados(df_consolidado, 'dados_usuarios_finais.xlsx')

if __name__ == "__main__":
    main()
