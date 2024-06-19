import pandas as pd
from database import *
from sqlalchemy import  select
from sqlalchemy.exc import SQLAlchemyError

def ler_excel(nome_arquivo):
    try:
        df = pd.read_excel(nome_arquivo, engine='openpyxl')
        
        df['consoles'] = df['consoles'].str.split('|')
        df['jogos_preferidos'] = df['jogos_preferidos'].str.split('|')
        return df
    except FileNotFoundError:
        print(f"Erro: O arquivo '{nome_arquivo}' não foi encontrado.")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo '{nome_arquivo}': {str(e)}")
        return None

def inserir_dados(df):
    session = None
    try:
        session = conectar_banco()

        for _, row in df.iterrows():
            usuario_existente = session.query(Usuario).filter_by(email=row['email']).first()
            if usuario_existente:
                continue
            
            # Inserir novo usuário
            usuario = Usuario(
                nome_completo=row['nome_completo'],
                data_nascimento=row['data_nascimento'],
                email=row['email'],
                cidade=row['cidade'],
                estado=row['estado']
            )
            session.add(usuario)

            # Inserir consoles do usuário
            for console_nome in row['consoles']:
                # Verificar se o console já existe
                console = session.query(Consoles).filter_by(nome=console_nome).first()
                if not console:
                    console = Consoles(nome=console_nome)
                    session.add(console)
                    session.flush()

                # Verificar se já existe associação entre usuário e console
                console_usuario = session.query(ConsoleUsuario).filter_by(id_usuario=usuario.id, id_console=console.id).first()
                if not console_usuario:
                    console_usuario = ConsoleUsuario(id_usuario=usuario.id, id_console=console.id)
                    session.add(console_usuario)

            # Inserir jogos preferidos do usuário
            for jogo_nome in row['jogos_preferidos']:
                jogo = session.query(Jogos).filter_by(nome=jogo_nome).first()
                if jogo is None:
                    jogo = Jogos(nome=jogo_nome)
                    session.add(jogo)

                usuario.jogos_preferidos.append(jogo)

            session.commit()

        print("Dados inseridos com sucesso!")
    except Exception as e:
        session.rollback()
        print(f"Erro ao inserir dados: {str(e)}")
    finally:
        desconectar_bd(session)


def inserir_dados_mercado_livre():
    # Conectar ao banco de dados 'dados_mercado_livre'
    engine_mercado_livre = create_engine('sqlite:///dados_mercado_livre.db')
    SessionMercadoLivre = sessionmaker(bind=engine_mercado_livre)
    session_mercado_livre = SessionMercadoLivre()

    # Ler dados da tabela 'jogos' no banco 'dados_mercado_livre'
    query = "SELECT * FROM jogos"
    df_jogos = pd.read_sql(query, engine_mercado_livre)

    session_jogos = conectar_banco()

    try:
        # Inserir dados na tabela 'jogos_mercado_livre'
        for _, row in df_jogos.iterrows():
            jogo_existente = session_jogos.query(JogosMercadoLivre).filter_by(permalink=row['permalink']).first()
            if not jogo_existente:
                jogo_mercado_livre = JogosMercadoLivre(
                    nome=row['nome'],
                    preco=row['preco'],
                    permalink=row['permalink']
                )
                session_jogos.add(jogo_mercado_livre)
                session_jogos.commit()
    except Exception as e:
        session_jogos.rollback()
        print(f"Erro ao inserir dados: {str(e)}")
    finally:
        session_mercado_livre.close()
        desconectar_bd(session_jogos)
        
def inserir_links_recomendados(df):
    session = None
    try:
        session = conectar_banco()
        lista_de_links = []

        for _, row in df.iterrows():
            links_jogos = []
            for jogo in row['jogos_preferidos']:
                try:
                    resultado = session.execute(
                        select(JogosMercadoLivre.permalink)
                        .where(JogosMercadoLivre.nome.contains(jogo))
                    ).fetchall()
                    for row in resultado:
                        links_jogos.append(row[0])
                except SQLAlchemyError as e:
                    print(f"Erro ao buscar links para o jogo {jogo}: {str(e)}")
            lista_de_links.append(links_jogos)

        df.insert(len(df.columns), column='links_recomendados', value=lista_de_links)
        print("Sucesso ao inserir links recomendados")
    except Exception as e:
        session.rollback()
        print(f"Erro ao inserir dados: {str(e)}")
    finally:
        desconectar_bd(session)