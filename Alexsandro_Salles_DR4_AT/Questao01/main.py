import pandas as pd
from bs4 import BeautifulSoup
import requests
from requests.exceptions import RequestException
from bs4 import FeatureNotFound
from io import StringIO

def get_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except RequestException as e:
        print(f"Erro ao fazer a requisição para {url}: {e}")
        return None

def parse_html(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        return soup
    except FeatureNotFound as e:
        print(f"Erro ao inicializar o BeautifulSoup: {e}")
        return None

def extract_table_playstation5(soup):
    try:
        table = soup.find('table', id='softwarelist')
        return table
    except AttributeError as e:
        print(f"Erro ao encontrar a tabela: {e}")
        return None

def extract_table_playstation4(soup):
    try:
        tables = soup.find_all('table', {'class': 'wikitable sortable'})
        
        if tables:
            return tables[0]
        else:
            print("Nenhuma tabela encontrada para PlayStation 4")
            return None
    except AttributeError as e:
        print(f"Erro ao encontrar a tabela: {e}")
        return None

def extract_table_xbox_series(soup):
    try:
        table = soup.find('table', id='softwarelist')
        return table
    except AttributeError as e:
        print(f"Erro ao encontrar a tabela: {e}")
        return None

def extract_table_xbox360(soup):
    try:
        table = soup.find('table', id='softwarelist')
        return table
    except AttributeError as e:
        print(f"Erro ao encontrar a tabela: {e}")
        return None

def extract_table_nintendo_switch(soup):
    try:
        table = soup.find('table', id='softwarelist')
        return table
    except AttributeError as e:
        print(f"Erro ao encontrar a tabela: {e}")
        return None

def parse_table(table):
    try:
        df = pd.read_html(StringIO(str(table)))[0]
        return df
    except ValueError as e:
        print(f"Erro ao converter a tabela em DataFrame: {e}")
        return None

def clean_data_ps5(df):
    try:
        df.drop(columns=['Addons', 'Ref.'], inplace=True, errors='ignore', level=0)
        
        df.dropna(inplace=True)  
        df.drop_duplicates(inplace=True)  
        df.reset_index(drop=True, inplace=True) 
        return df
    except Exception as e:
        print(f"Erro ao limpar os dados do PlayStation 5: {e}")
        return None

def clean_data_ps4(df):
    try:
        df.drop(columns=['Referências'], inplace=True, errors='ignore')
       
        df.dropna(inplace=True) 
        df.drop_duplicates(inplace=True) 
        df.reset_index(drop=True, inplace=True) 
        return df
    except Exception as e:
        print(f"Erro ao limpar os dados do PlayStation 4: {e}")
        return None

def clean_data_xbox_series(df):
    try:
        df.drop(columns=['Complementos', 'Ref'], inplace=True, errors='ignore', level=0)
       
        df.dropna(inplace=True)  
        df.drop_duplicates(inplace=True)  
        df.reset_index(drop=True, inplace=True)  
        return df
    except Exception as e:
        print(f"Erro ao limpar os dados do Xbox Series X/S: {e}")
        return None

def clean_data_xbox360(df):
    try:
        df.drop(columns=['Addons', 'Xbox One', 'Ref.'], inplace=True, errors='ignore', level=0)
       
        df.dropna(inplace=True)  
        df.drop_duplicates(inplace=True) 
        df.reset_index(drop=True, inplace=True)  
        return df
    except Exception as e:
        print(f"Erro ao limpar os dados do Xbox 360: {e}")
        return None

def clean_data_switch(df):
    try:
        df.drop(columns=['Obs.', 'Ref.'], inplace=True, errors='ignore', level=0)
        
        df.dropna(inplace=True)  
        df.drop_duplicates(inplace=True)  
        df.reset_index(drop=True, inplace=True)  
        return df
    except Exception as e:
        print(f"Erro ao limpar os dados do Nintendo Switch: {e}")
        return None

def export_data(df, filename_base):
    try:
        df.to_csv(f"Questao01/{filename_base}.csv", index=False)
        df.to_json(f"Questao01/{filename_base}.json", orient='records', force_ascii=False)
        df.to_excel(f"Questao01/{filename_base}.xlsx")
        print(f"Dados exportados como {filename_base}.csv, {filename_base}.json e {filename_base}.xlsx")
    except Exception as e:
        print(f"Erro ao exportar os dados: {e}")

def main():
    urls = [
        ("https://pt.wikipedia.org/wiki/Lista_de_jogos_para_PlayStation_5", "ps5_games", extract_table_playstation5, clean_data_ps5),
        ("https://pt.wikipedia.org/wiki/Lista_de_jogos_para_PlayStation_4", "ps4_games", extract_table_playstation4, clean_data_ps4),
        ("https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Xbox_Series_X_e_Series_S", "xbox_series_games", extract_table_xbox_series, clean_data_xbox_series),
        ("https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Xbox_360", "xbox_360_games", extract_table_xbox360, clean_data_xbox360),
        ("https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Nintendo_Switch", "switch_games", extract_table_nintendo_switch, clean_data_switch)
    ]

    for url, filename_base, extract_function, clean_function in urls:
        print(f"Processando {url}")
       
        html = get_html(url)
        if html:
            soup = parse_html(html)
            if soup:
                table = extract_function(soup)
                if table:
                    df = parse_table(table)
                    if df is not None:
                        clean_df = clean_function(df)
                        if clean_df is not None:
                            export_data(clean_df, filename_base)
                            

if __name__ == "__main__":
    main()
