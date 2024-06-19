import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# URL base da API do Mercado Livre
base_url = "https://api.mercadolibre.com/sites/MLB/search"

Base = declarative_base()
Base_mercado_Livre = declarative_base()

class JogoBase(Base_mercado_Livre):
    __tablename__ = 'jogos_totais'
    id = Column(Integer, primary_key=True)
    jogo = Column(String)
       
class Jogo(Base):
    __tablename__ = 'jogos'
            
    id = Column(Integer, primary_key=True)
    nome = Column(String)
    preco = Column(String)
    permalink = Column(String)
                
def criar_sessao(engine):
    Session = sessionmaker(bind=engine)
    return Session()

def mercado_livre(jogos, session):
    for jogo in jogos:
        query_params = {
            'category': 'MLB186456',  
            'q': jogo
        }
        
        try:
            # Fazer a solicitação HTTP para a API do Mercado Livre
            response = requests.get(base_url, params=query_params)
            response.raise_for_status()  # Lança exceção para erros HTTP
            
            # Extrair dados do JSON da resposta
            data = response.json()
            
            # Verificar se há resultados e extrair as informações desejadas
            if data['results']:
                resultado = data['results'][0]
                
                nome = resultado.get('title', 'N/A')
                preco = resultado.get('price', 'N/A')
                permalink = resultado.get('permalink', 'N/A')
                
                # Criar um novo objeto Jogo e adicioná-lo à sessão
                novo_jogo = Jogo(nome=nome, preco=str(preco), permalink=permalink)
                session.add(novo_jogo)
                session.commit()
                print(f"Dados do jogo '{jogo}' salvos com sucesso.")
            else:
                print(f"Nenhum resultado encontrado para o jogo '{jogo}'.")
                print(f"URL: {response.url}")
                print(f"Resposta da API: {data}")
        
        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer solicitação HTTP para o jogo '{jogo}': {str(e)}")
        
        except Exception as e:
            print(f"Erro ao processar jogo '{jogo}': {str(e)}")
    
    session.close()
    
def ler_jogos_analise(session):
    try:
        # Consultar e carregar os nomes dos jogos da tabela
        query = session.query(JogoBase.jogo).all()
        jogos = [jogo[0] for jogo in query]
        return jogos
    except Exception as e:
        print(f"Erro ao carregar dados da tabela 'jogos_totais': {str(e)}")
        return []

def main():
    # Mercado Livre
    engine_mercado_livre = create_engine('sqlite:///dados_mercado_livre.db')
    Base.metadata.create_all(engine_mercado_livre)
    session_mercado_livre = criar_sessao(engine_mercado_livre)
    
    # Conexão com o banco de dados de análise de jogos
    engine_analise_jogos = create_engine('sqlite:///analise_jogos.db')
    Base_mercado_Livre.metadata.create_all(engine_analise_jogos)
    session_analise_jogos = criar_sessao(engine_analise_jogos)
    
    # Leitura da lista de jogos do banco de dados de análise de jogos
    jogos_analise = ler_jogos_analise(session_analise_jogos)
    
    # Chamar função para consultar a API e salvar os dados
    mercado_livre(jogos_analise, session_mercado_livre)

    session_mercado_livre.close()
    session_analise_jogos.close()

if __name__ == "__main__":
    main()
