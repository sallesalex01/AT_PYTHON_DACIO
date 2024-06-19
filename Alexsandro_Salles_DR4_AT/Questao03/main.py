import pandas as pd
from sqlalchemy import create_engine

# Função para ler o arquivo Excel com a coluna 'jogos_preferidos'
def ler_excel(nome_arquivo):
    try:
        df = pd.read_excel(nome_arquivo, engine='openpyxl')
        return df
    except FileNotFoundError:
        print(f"Erro: O arquivo '{nome_arquivo}' não foi encontrado.")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo '{nome_arquivo}': {str(e)}")
        return None

# Função para separar os jogos preferidos de cada usuário
def limpar_dados(df):
    if df is None:
        return None
    
    df = df.astype(str)
    
    # Dividir a coluna 'jogos_preferidos' em uma lista de jogos
    df['jogos_preferidos'] = df['jogos_preferidos'].str.split('|')
    df['consoles'] = df['consoles'].str.split('|')
    return df

# Função para realizar operações com sets e gerar análises
def analisar_jogos(df):
    if df is None:
        print("Data frame vazio")
        return None, None, None
    
    # Conjunto para armazenar todos os jogos relatados
    jogos_totais = set()

    # Dicionário para contar a frequência de cada jogo
    frequencia_jogos = {}

    # Preencher o conjunto e o dicionário com os dados do DataFrame
    for _, row in df.iterrows():
        jogos_usuario = set(row['jogos_preferidos'])

        # Atualizar o conjunto de todos os jogos
        jogos_totais.update(jogos_usuario)

        # Atualizar a frequência de cada jogo
        for jogo in jogos_usuario:
            if jogo in frequencia_jogos:
                frequencia_jogos[jogo] += 1
            else:
                frequencia_jogos[jogo] = 1

    # Filtrar jogos relatados por apenas um usuário
    jogos_um_usuario = {jogo for jogo, freq in frequencia_jogos.items() if freq == 1}

    # Encontrar jogos com mais aparições
    max_aparicoes = max(frequencia_jogos.values())
    jogos_max = {jogo for jogo, freq in frequencia_jogos.items() if freq == max_aparicoes}

    return jogos_totais, jogos_um_usuario, jogos_max

def exportar_para_sqlite(jogos_totais, jogos_um_usuario, jogos_max, nome_banco='analise_jogos.db'):
    try:
        # Cria a engine do SQLAlchemy para o banco de dados SQLite
        engine = create_engine(f'sqlite:///{nome_banco}')
        
        # Criar DataFrames a partir dos sets
        df_jogos_totais = pd.DataFrame(list(jogos_totais), columns=['jogo'])
        df_jogos_um_usuario = pd.DataFrame(list(jogos_um_usuario), columns=['jogo'])
        df_jogos_max_aparicoes = pd.DataFrame(list(jogos_max), columns=['jogo'])
        
        # Exportar DataFrames para SQLite, criando ou substituindo as tabelas
        df_jogos_totais.to_sql('jogos_totais', engine, index=False, if_exists='replace')
        df_jogos_um_usuario.to_sql('jogos_um_usuario', engine, index=False, if_exists='replace')
        df_jogos_max_aparicoes.to_sql('jogos_max_aparicoes', engine, index=False, if_exists='replace')

        print(f'Dados exportados para o banco de dados SQLite: {nome_banco}')
    except Exception as e:
        print(f'Ocorreu um erro ao exportar para SQLite: {str(e)}')

def main():
    df = ler_excel('dados_usuarios_finais.xlsx')
    df_limpo = limpar_dados(df)
    jogos_totais, jogos_um_usuario, jogos_max = analisar_jogos(df_limpo)
    
    exportar_para_sqlite(jogos_totais, jogos_um_usuario, jogos_max)
    
if __name__ == "__main__":
    main()