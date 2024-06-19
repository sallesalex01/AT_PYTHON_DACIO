from sqlalchemy import  Column, Integer, String, Float, ForeignKey, Table, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

class Usuario(Base):
    __tablename__ = 'usuarios'

    id = Column(Integer, primary_key=True)
    nome_completo = Column(String)
    data_nascimento = Column(String)
    email = Column(String)
    cidade = Column(String)
    estado = Column(String)

    consoles = relationship('ConsoleUsuario', backref='usuario')
    jogos_preferidos = relationship('Jogos', secondary='jogos_preferidos')

class JogosMercadoLivre(Base):
    __tablename__ = 'jogos_mercado_livre'

    id = Column(Integer, primary_key=True)
    nome = Column(String)
    preco = Column(Integer)
    permalink = Column(String)

class Jogos(Base):
    __tablename__ = 'jogos'
    
    id = Column(Integer, primary_key=True)
    nome = Column(String)

class JogosPreferidos(Base):
    __tablename__ = 'jogos_preferidos'

    id = Column(Integer, primary_key=True)
    id_usuario = Column(Integer, ForeignKey('usuarios.id'))
    id_jogo = Column(Integer, ForeignKey('jogos.id'))

class Consoles(Base):
    __tablename__ = 'consoles'

    id = Column(Integer, primary_key=True)
    nome = Column(String)

class ConsoleUsuario(Base):
    __tablename__ = 'console_usuario'

    id_usuario = Column(Integer, ForeignKey('usuarios.id'), primary_key=True)
    id_console = Column(Integer, ForeignKey('consoles.id'), primary_key=True)
    
def conectar_bd_mercado_livre():
    session = None
    try:
        engine_mercado_livre = create_engine('sqlite:///dados_mercado_livre.db')
        Session = sessionmaker(bind=engine_mercado_livre)
        session = Session()
    except ImportError as ie:
        print(f"Erro de importação ao conectar ao banco de dados: {str(ie)}")
    except ConnectionError as ce:
        print(f"Erro de conexão ao conectar ao banco de dados: {str(ce)}")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {str(e)}")
    return session

def conectar_bd_jogos():
    session = None
    try:
        engine_jogos = create_engine('sqlite:///analise_jogos.db')
        Session = sessionmaker(bind=engine_jogos)
        session = Session()
    except ImportError as ie:
        print(f"Erro de importação ao conectar ao banco de dados: {str(ie)}")
    except ConnectionError as ce:
        print(f"Erro de conexão ao conectar ao banco de dados: {str(ce)}")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {str(e)}")
    return session

def conectar_banco():
    session = None
    try:
        engine_sistema = create_engine('sqlite:///sistema.db')
        Session = sessionmaker(bind=engine_sistema)
        Base.metadata.create_all(engine_sistema)
        session = Session()
        return session
    except ImportError as ie:
        print(f"Erro de importação ao conectar ao banco de dados: {str(ie)}")
        return session
    except ConnectionError as ce:
        print(f"Erro de conexão ao conectar ao banco de dados: {str(ce)}")
        return session
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {str(e)}")
        return session
     
def desconectar_bd(session):
    if session:
        try:
            session.close()
        except Exception as e:
            print(f"Erro ao fechar sessão do banco de dados: {str(e)}")


