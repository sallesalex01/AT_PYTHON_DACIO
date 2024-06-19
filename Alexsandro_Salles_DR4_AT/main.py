from dados import *
from database import *
from Questao03.main import analisar_jogos
from Questao04.main import ler_jogos_analise, mercado_livre

def exportar_dados(df, nome_arquivo):
    try:
        df.to_csv(f"Sistema/{nome_arquivo}.csv", index=False)
        df.to_json(f"Sistema/{nome_arquivo}.json", orient='records', force_ascii=False)
        df.to_excel(f"Sistema/{nome_arquivo}.xlsx")
        print(f'Dados exportados com sucesso para {nome_arquivo}')
    except Exception as e:
        print(f'Ocorreu um erro ao exportar para Excel: {str(e)}')

def request_mercado_livre():
    session_jogos = conectar_bd_jogos()
    jogos_analise = ler_jogos_analise(session_jogos)
    
    session_mercado_livre = conectar_bd_mercado_livre()
    mercado_livre(jogos_analise, session_mercado_livre)
    
    inserir_dados_mercado_livre()

def main():
    df_usuarios = ler_excel('dados_usuarios_finais.xlsx')
    
    print(df_usuarios)
    
    #insere dados do usuario
    inserir_dados(df_usuarios)
    
    #insere dados do mercado livre
    inserir_dados_mercado_livre()
    
    #insere links recomendados para o usuario no dataframe
    inserir_links_recomendados(df_usuarios)
    
    #exportar dados
    exportar_dados(df_usuarios, 'dados_finais')
    
       # Análise dos jogos
    jogos_totais, jogos_um_usuario, jogos_max = analisar_jogos(df_usuarios)
    
    while True:
        print("\n### Menu ###")
        print("1. Ver jogos do momento")
        print("2. Atualizar base de jogos")
        print("3. Sair")
        
        escolha = input("Escolha uma opção: ")
        
        if escolha == '1':
            print("\nJogos do Momento:")
            print(jogos_max)
                
        elif escolha == '2':
            print("\nAtualizando base de jogos...")
            request_mercado_livre()
            print("Base de jogos atualizada com sucesso.")
            
        elif escolha == '3':
            print("Saindo do programa.")
            break
        
        else:
            print("Opção inválida. Escolha novamente.")

if __name__ == "__main__":
    main()
