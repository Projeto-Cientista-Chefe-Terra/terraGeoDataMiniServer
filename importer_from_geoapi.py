# importer_from_geoapi.py
# Script para coletar e processar dados da GeoAPI do Ceará
# Autor: Wellington Sarmento
# Data: 2024-06-20
# Versão: 1.0.0

# import os
# import logging
# from datetime import datetime
# from collections import defaultdict
# import requests
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
# import pandas as pd
# import csv
# import re
# import pyproj
# from shapely import wkb
# from shapely.geometry import shape, mapping
# from shapely.ops import transform
# import unidecode
# import curses
# import sys
# from time import sleep


# # Configuração de logging
# log_filename = datetime.now().strftime("logs/geoapi_importer_%Y_%m_%d_%H_%M.log")
# os.makedirs("logs", exist_ok=True)
# logging.basicConfig(
#     filename=log_filename,
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger()

# # Lista de municípios
# municipios = [
#     "ABAIARA", "ACARAPE", "ACARAU", "ACOPIARA", "AIUABA", "ALCANTARAS", "ALTANEIRA", "ALTO%20SANTO", 
#     "AMONTADA", "ANTONINA%20DO%20NORTE", "APUIARES", "AQUIRAZ", "ARACATI", "ARACOIABA", "ARARENDA", 
#     "ARARIPE", "ARATUBA", "ARNEIROZ", "ASSARE", "AURORA", "BAIXIO", "BANABUIU", "BARBALHA", "BARREIRA", 
#     "BARRO", "BARROQUINHA", "BATURITE", "BEBERIBE", "BELA%20CRUZ", "BOA%20VIAGEM", "BREJO%20SANTO", 
#     "CAMOCIM", "CAMPOS%20SALES", "CANINDE", "CAPISTRANO", "CARIDADE", "CARIRE", "CARIRIACU", "CARIUS", 
#     "CARNAUBAL", "CASCAVEL", "CATARINA", "CATUNDA", "CAUCAIA", "CEDRO", "CHAVAL", "CHORO", "CHOROZINHO", 
#     "COREAU", "CRATEUS", "CRATO", "CROATA", "CRUZ", "DEPUTADO%20IRAPUAN%20PINHEIRO", "ERERE", "EUSEBIO", 
#     "FARIAS%20BRITO", "FORQUILHA", "FORTALEZA", "FORTIM", "FRECHEIRINHA", "GENERAL%20SAMPAIO", "GRACA", 
#     "GRANJA", "GRANJEIRAS", "GROAIRAS", "GUAIUBA", "GUARACIABA%20DO%20NORTE", "GUARAMIRANGA", "HIDROLANDIA", 
#     "HORIZONTE", "IBARETAMA", "IBIAPINA", "IBICUITINGA", "ICAPUI", "ICO", "IGUATU", "INDEPENDENCIA", 
#     "IPAPORANGA", "IPAUMIRIM", "IPU", "IPUEIRAS", "IRACEMA", "IRAUCUBA", "ITAICABA", "ITAITINGA", 
#     "ITAPAJE", "ITAPIPOCA", "ITAPIUNA", "ITAREMA", "ITATIRA", "JAGUARETAMA", "JAGUARIBARA", "JAGUARIBE", 
#     "JAGUARUANA", "JARDIM", "JATI", "JIJOCA%20DE%20JERICOACOARA", "JUAZEIRO%20DO%20NORTE", "JUCAS", 
#     "LAVRAS%20DA%20MANGABEIRA", "LIMOEIRAS%20DO%20NORTE", "MADALENA", "MARACANAU", "MARANGUAPE", "MARCO", 
#     "MARTINOPOLE", "MASSAPE", "MAURITI", "MERUOCA", "MILAGRES", "MILHA", "MIRAIMA", "MISSAO%20VELHA", 
#     "MOMBACA", "MONSENHOR%20TABOSA", "MORADA%20NOVA", "MORAUJO", "MORRINHOS", "MUCAMBO", "MULUNGU", 
#     "NOVA%20OLINDA", "NOVA%20RUSSAS", "NOVO%20ORIENTE", "OCARA", "OROS", "PACAJUS", "PACATUBA", "PACOTI", 
#     "PACUJA", "PALHANO", "PALMACIA", "PARACURU", "PARAIPABA", "PARAMBU", "PARAMOTI", "PEDRA%20BRANCA", 
#     "PENAFORTE", "PENTECOSTE", "PEREIRO", "PINDORETAMA", "PIQUET%20CARNEIRO", "PIRES%20FERREIRA", "PORANGA", 
#     "PORTEIRAS", "POTENGI", "POTIRETAMA", "QUITERIANOPOLIS", "QUIXADA", "QUIXELO", "QUIXERAMOBIM", "QUIXERE", 
#     "REDENCAO", "RERIUTABA", "RUSSAS", "SABOEIRO", "SALITRE", "SANTA%20QUITERIA", "SANTANA%20DO%20ACARAU", 
#     "SANTANA%20DO%20CARIRI", "SAO%20BENEDITO", "SAO%20GONCALO%20DO%20AMARANTE", "SAO%20JOAO%20DO%20JAGUARIBE", 
#     "SAO%20LUIS%20DO%20CURU", "SENADOR%20POMPEU", "SENADOR%20SA", "SOBRAL", "SOLONOPOLE", "TABULEIRO%20DO%20NORTE", 
#     "TAMBORIL", "TARRAFAS", "TAUA", "TEJUCUOCA", "TIANGUA", "TRAIRI", "TURURU", "UBAJARA", "UMARI", "UMIRIM", 
#     "URUBURETAMA", "URUOCA", "VARJOTA", "VARZEA%20ALEGRE", "VICOSA%20DO%20CEARA"
# ]

# # Estatísticas
# stats = defaultdict(int, {
#     'municipios_total': len(municipios),
#     'municipios_processados': 0,
#     'municipios_falha': 0,
#     'municipios_sem_dados': 0,
#     'municipios_com_erros': 0,
#     'registros_processados': 0,
#     'registros_invalidos': 0,
#     'erros_comunicacao': 0,
#     'imoveis_com_problemas_por_municipio': defaultdict(int)
# })

# # Configurações
# try:
#     import config
#     TOKEN_GEOAPI = config.settings.TOKEN_GEOAPI
# except:
#     TOKEN_GEOAPI = os.environ.get('TOKEN_GEOAPI')
#     if not TOKEN_GEOAPI:
#         raise ValueError("Token da GeoAPI não encontrado")


# # Classe para gerenciar a interface ncurses
# class ProgressMonitor:
#     def __init__(self):
#         self.stdscr = curses.initscr()
#         curses.noecho()
#         curses.cbreak()
#         self.stdscr.keypad(True)
#         self.progress_win = curses.newwin(10, 80, 0, 0)
        
#     def update_progress(self, municipio_atual, total_processados, stats):
#         self.progress_win.clear()
#         self.progress_win.addstr(0, 0, "PROGRESSO DA COLETA DE DADOS")
#         self.progress_win.addstr(1, 0, f"Município atual: {municipio_atual}")
#         self.progress_win.addstr(2, 0, f"Processados: {total_processados}/{stats['municipios_total']}")
        
#         # Barra de progresso
#         progresso = int((total_processados/stats['municipios_total'])*50)
#         barra = "[" + "#" * progresso + " " * (50 - progresso) + "]"
#         self.progress_win.addstr(3, 0, barra)
        
#         # Estatísticas
#         self.progress_win.addstr(5, 0, "ESTATÍSTICAS:")
#         self.progress_win.addstr(6, 0, f"Municípios com falha: {stats['municipios_falha']}")
#         self.progress_win.addstr(7, 0, f"Erros de comunicação: {stats['erros_comunicacao']}")
#         self.progress_win.addstr(8, 0, f"Registros processados: {stats['registros_processados']}")
        
#         self.progress_win.refresh()
        
#     def close(self):
#         curses.nocbreak()
#         self.stdscr.keypad(False)
#         curses.echo()
#         curses.endwin()

# # Cliente GeoAPI com retry
# class GeoAPIClient:
#     BASE_URL = "http://geoapi.idace.ce.gov.br/geoapi/pessoa/municipio/"
    
#     def __init__(self):
#         self.token = TOKEN_GEOAPI
#         self.headers = {
#             'Authorization': f'Bearer {self.token}',
#             'Content-Type': 'application/json'
#         }
    
#     @retry(
#         stop=stop_after_attempt(5),
#         wait=wait_exponential(multiplier=1, min=2, max=30),
#         retry=retry_if_exception_type(requests.exceptions.RequestException),
#         before_sleep=lambda retry_state: logger.warning(
#             f"Tentativa {retry_state.attempt_number} falhou. Retentando em {retry_state.next_action.sleep} segundos."
#         )
#     )
#     def fetch_data(self, municipio, pagina=0, tamanho=10000):
#         url = f"{self.BASE_URL}{municipio}?pagina={pagina}&tamanho={tamanho}&ordenarPor=proprietario"
#         try:
#             response = requests.get(url, headers=self.headers, timeout=30)
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             logger.error(f"Erro de comunicação com a API: {str(e)}")
#             stats['erros_comunicacao'] += 1
#             raise

# # Função para normalizar nome do município
# def normalizar_nome_municipio(nome):
#     # Remove acentos e caracteres especiais
#     nome_sem_acentos = unidecode.unidecode(nome)
#     # Converte para minúsculo e substitui espaços por underscore
#     nome_normalizado = nome_sem_acentos.lower().replace(' ', '_')
#     return nome_normalizado

# # Função para converter geometria de EPSG:31984 para EPSG:4326
# def converter_geometria(geom_hex, epsg_origem=31984, epsg_destino=4326):
#     try:
#         # Converte hexadecimal para geometria
#         geom_bytes = bytes.fromhex(geom_hex)
#         geom = wkb.loads(geom_bytes)
        
#         # Define os sistemas de coordenadas
#         projeto_origem = pyproj.CRS(f'EPSG:{epsg_origem}')
#         projeto_destino = pyproj.CRS(f'EPSG:{epsg_destino}')
        
#         # Cria transformador
#         transformar = pyproj.Transformer.from_crs(
#             projeto_origem, projeto_destino, always_xy=True
#         ).transform
        
#         # Aplica transformação
#         geom_transformada = transform(transformar, geom)
        
#         # Converte para hexadecimal no formato WKB
#         return geom_transformada.wkb_hex
#     except Exception as e:
#         logger.error(f"Erro ao converter geometria: {str(e)}")
#         return None

# # Função para calcular área a partir da geometria
# def calcular_area(geom_hex, epsg=31984):
#     try:
#         # Converte hexadecimal para geometria
#         geom_bytes = bytes.fromhex(geom_hex)
#         geom = wkb.loads(geom_bytes)
        
#         # Define o sistema de coordenadas para cálculo de área
#         projeto = pyproj.CRS(f'EPSG:{epsg}')
#         transformer = pyproj.Transformer.from_crs(
#             projeto, projeto, always_xy=True
#         ).transform
        
#         # Aplica transformação para garantir coordenadas planas
#         geom_proj = transform(transformer, geom)
        
#         # Calcula área em metros quadrados
#         area_m2 = geom_proj.area
        
#         # Converte para hectares
#         area_ha = area_m2 / 10000
        
#         return area_ha
#     except Exception as e:
#         logger.error(f"Erro ao calcular área: {str(e)}")
#         return None

# # Função para verificar padrões em texto
# def verificar_padrao(texto, padroes):
#     if not texto:
#         return False
    
#     texto = str(texto).lower()
#     for padrao in padroes:
#         if re.search(padrao, texto):
#             return True
#     return False

# # Função principal para coleta de dados
# def coletar_dados(progress_monitor):
#     logger.info("Iniciando coleta de dados da GeoAPI")
#     geo_client = GeoAPIClient()
    
#     csv_filename = "datasets/malha_fundiaria_bruto.csv"
#     fieldnames = [
#         'lote_id', 'nome_municipio_original', 'nome_proprietario', 'imovel',
#         'codigo_distrito', 'ponto_de_referencia', 'codigo_municipio', 
#         'geometry_31984', 'centroide', 'nome_distrito', 'data_criacao_lote',
#         'data_modificacao_lote', 'situacao_juridica', 'numero_incra',
#         'numero_titulo', 'numero_lote'
#     ]
    
#     with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()
        
#         for municipio in municipios:
#             clean_name = municipio.replace("%20", " ")
#             logger.info(f"Processando município: {clean_name}")
            
#             # Atualiza display de progresso
#             progress_monitor.update_progress(
#                 clean_name, 
#                 stats['municipios_processados'], 
#                 stats
#             )
            
#             try:
#                 data = geo_client.fetch_data(municipio)
                
#                 if not data or not isinstance(data, list):
#                     logger.warning(f"Nenhum dado válido para {clean_name}")
#                     stats['municipios_sem_dados'] += 1
#                     continue
                    
#                 problemas_municipio = 0
                
#                 for item in data:
#                     try:
#                         if not item.get('multipolygon'):
#                             logger.warning(f"Registro {item.get('id')} sem geometria")
#                             stats['registros_invalidos'] += 1
#                             problemas_municipio += 1
#                             continue
                            
#                         record = {
#                             'lote_id': item.get('loteId'),
#                             'nome_municipio_original': item.get('municipio'),
#                             'nome_proprietario': item.get('proprietario'),
#                             'imovel': item.get('imovel'),
#                             'codigo_distrito': item.get('codigoDistrito'),
#                             'ponto_de_referencia': item.get('pontoDeReferencia'),
#                             'codigo_municipio': item.get('codigoMunicipio'),
#                             'geometry_31984': item.get('multipolygon'),
#                             'centroide': item.get('centroide'),
#                             'nome_distrito': item.get('nomeDistrito'),
#                             'data_criacao_lote': item.get('dhc'),
#                             'data_modificacao_lote': item.get('dhm'),
#                             'situacao_juridica': item.get('situacaoJuridica'),
#                             'numero_incra': item.get('sncr'),
#                             'numero_titulo': item.get('titulo'),
#                             'numero_lote': item.get('numero')
#                         }
                        
#                         writer.writerow(record)
#                         stats['registros_processados'] += 1
                        
#                     except Exception as e:
#                         logger.error(f"Erro no registro {item.get('id')}: {str(e)}")
#                         stats['registros_invalidos'] += 1
#                         problemas_municipio += 1
                
#                 # Registra número de imóveis com problemas neste município
#                 stats['imoveis_com_problemas_por_municipio'][clean_name] = problemas_municipio
#                 logger.info(f"{clean_name}: {len(data)} registros processados, {problemas_municipio} com problemas")
                
#             except Exception as e:
#                 logger.error(f"Erro em {clean_name}: {str(e)}")
#                 stats['municipios_com_erros'] += 1
#                 stats['municipios_falha'] += 1
            
#             stats['municipios_processados'] += 1
    
#     logger.info(f"Coleta concluída. Arquivo {csv_filename} gerado.")
#     return csv_filename

# # Função para processar dados adicionais
# def processar_dados_adicionais(csv_filename):
#     logger.info("Iniciando processamento de dados adicionais")
    
#     # Carrega dados do CSV bruto
#     df = pd.read_csv(csv_filename,low_memory=False)
    
#     # Carrega dados dos módulos fiscais
#     modulos_df = pd.read_csv("datasets/regioes_municipios_modulos_fiscais_ceara.csv", low_memory=False)
    
#     # 1. Normaliza nome do município
#     df['nome_municipio'] = df['nome_municipio_original'].apply(normalizar_nome_municipio)
    
#     # 2. Adiciona região administrativa
#     df = df.merge(
#         modulos_df[['nome_municipio', 'regiao_administrativa']], 
#         on='nome_municipio', 
#         how='left'
#     )
    
#     # 3. Adiciona módulo fiscal
#     df = df.merge(
#         modulos_df[['nome_municipio', 'modulo_fiscal']], 
#         on='nome_municipio', 
#         how='left'
#     )
    
#     # 4. Calcula área
#     df['area'] = df['geometry_31984'].apply(calcular_area)
    
#     # 5. Classifica por categoria
#     def classificar_categoria(row):
#         if pd.isna(row['area']) or pd.isna(row['modulo_fiscal']):
#             return 'Indefinido'
        
#         area = row['area']
#         modulo = row['modulo_fiscal']
        
#         if area < 1 * modulo:
#             return 'Pequena Propriedade < 1 MF'
#         elif area < 4 * modulo:
#             return 'Pequena Propriedade'
#         elif area <= 15 * modulo:
#             return 'Média Propriedade'
#         else:
#             return 'Grande Propriedade'
    
#     df['categoria'] = df.apply(classificar_categoria, axis=1)
    
#     # 6. Converte geometria para EPSG:4326
#     df['geometry'] = df['geometry_31984'].apply(
#         lambda x: converter_geometria(x, 31984, 4326) if pd.notna(x) else None
#     )
    
#     # 7. Verifica se é quilombo
#     padroes_quilombo = [r'quilomb']
#     df['ehquilombo'] = df.apply(
#         lambda x: verificar_padrao(x['nome_proprietario'], padroes_quilombo) or 
#                  verificar_padrao(x['imovel'], padroes_quilombo), 
#         axis=1
#     )
    
#     # 8. Verifica se é indígena
#     padroes_indigena = [r'indígen', r'indigen']
#     df['ehindigena'] = df.apply(
#         lambda x: verificar_padrao(x['nome_proprietario'], padroes_indigena) or 
#                  verificar_padrao(x['imovel'], padroes_indigena), 
#         axis=1
#     )
    
#     # 9. Verifica se é assentamento
#     padroes_assentamento = [r'assenta']
#     df['ehassentamento'] = df.apply(
#         lambda x: verificar_padrao(x['nome_proprietario'], padroes_assentamento) or 
#                  verificar_padrao(x['imovel'], padroes_assentamento), 
#         axis=1
#     )
    
#     # Salva o dataset final
#     output_filename = "datasets/malha_fundiaria.csv"
#     df.to_csv(output_filename, index=False, encoding='utf-8')
    
#     logger.info(f"Processamento concluído. Arquivo {output_filename} gerado.")
#     return output_filename

# # Função para imprimir estatísticas
# def print_stats(stats):
#     print("\n=== RESUMO ESTATÍSTICO ===")
#     print(f"• Municípios totais: {stats['municipios_total']}")
#     print(f"• Municípios processados: {stats['municipios_processados']}")
#     print(f"• Municípios com falha: {stats['municipios_falha']}")
#     print(f"• Municípios sem dados: {stats['municipios_sem_dados']}")
#     print(f"• Erros de comunicação: {stats['erros_comunicacao']}")
#     print(f"• Registros processados: {stats['registros_processados']}")
#     print(f"• Registros inválidos: {stats['registros_invalidos']}")
    
#     print("\n• Imóveis com problemas por município:")
#     for municipio, problemas in stats['imoveis_com_problemas_por_municipio'].items():
#         print(f"  {municipio}: {problemas} problemas")
    
#     print("=========================\n")

# # Função principal
# def main():
#     # Inicializa monitor de progresso
#     progress_monitor = ProgressMonitor()
    
#     try:
#         logger.info("Iniciando processo completo de importação e processamento")
        
#         # Coleta dados da API e gera CSV bruto
#         csv_bruto = coletar_dados(progress_monitor)
        
#         # Fecha interface ncurses antes do processamento adicional
#         progress_monitor.close()
        
#         # Processa dados adicionais
#         csv_processado = processar_dados_adicionais(csv_bruto)
        
#         # Exibe estatísticas finais
#         print_stats(stats)
#         logger.info("\n==== RESUMO FINAL ====")
#         for key, value in stats.items():
#             if key != 'imoveis_com_problemas_por_municipio':
#                 logger.info(f"{key}: {value}")
        
#         # Log detalhado de problemas por município
#         logger.info("Problemas por município:")
#         for municipio, problemas in stats['imoveis_com_problemas_por_municipio'].items():
#             logger.info(f"{municipio}: {problemas} problemas")
            
#         logger.info("Processo concluído")
        
#     except Exception as e:
#         progress_monitor.close()
#         logger.error(f"Erro no processo principal: {str(e)}")
#         print(f"Erro: {str(e)}")
#         sys.exit(1)
        
#     except KeyboardInterrupt:
#         progress_monitor.close()
#         logger.info("Processo interrompido pelo usuário")
#         print("Processo interrompido")
#         sys.exit(0)

# if __name__ == "__main__":
#     main()

# importer_from_geoapi.py
# Script para coletar e processar dados da GeoAPI do Ceará
# Autor: Wellington Sarmento
# Data: 2024-06-20
# Versão: 1.0.0

import os
import logging
from datetime import datetime
from collections import defaultdict
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pandas as pd
import csv
import re
import unidecode
import curses
import sys
from time import sleep
import shutil


# Configuração de logging
log_filename = datetime.now().strftime("logs/geoapi_importer_%Y_%m_%d_%H_%M.log")
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Lista de municípios
municipios = [
    "ABAIARA", "ACARAPE", "ACARAU", "ACOPIARA", "AIUABA", "ALCANTARAS", "ALTANEIRA", "ALTO%20SANTO", 
    "AMONTADA", "ANTONINA%20DO%20NORTE", "APUIARES", "AQUIRAZ", "ARACATI", "ARACOIABA", "ARARENDA", 
    "ARARIPE", "ARATUBA", "ARNEIROZ", "ASSARE", "AURORA", "BAIXIO", "BANABUIU", "BARBALHA", "BARREIRA", 
    "BARRO", "BARROQUINHA", "BATURITE", "BEBERIBE", "BELA%20CRUZ", "BOA%20VIAGEM", "BREJO%20SANTO", 
    "CAMOCIM", "CAMPOS%20SALES", "CANINDE", "CAPISTRANO", "CARIDADE", "CARIRE", "CARIRIACU", "CARIUS", 
    "CARNAUBAL", "CASCAVEL", "CATARINA", "CATUNDA", "CAUCAIA", "CEDRO", "CHAVAL", "CHORO", "CHOROZINHO", 
    "COREAU", "CRATEUS", "CRATO", "CROATA", "CRUZ", "DEPUTADO%20IRAPUAN%20PINHEIRO", "ERERE", "EUSEBIO", 
    "FARIAS%20BRITO", "FORQUILHA", "FORTALEZA", "FORTIM", "FRECHEIRINHA", "GENERAL%20SAMPAIO", "GRACA", 
    "GRANJA", "GRANJEIRAS", "GROAIRAS", "GUAIUBA", "GUARACIABA%20DO%20NORTE", "GUARAMIRANGA", "HIDROLANDIA", 
    "HORIZONTE", "IBARETAMA", "IBIAPINA", "IBICUITINGA", "ICAPUI", "ICO", "IGUATU", "INDEPENDENCIA", 
    "IPAPORANGA", "IPAUMIRIM", "IPU", "IPUEIRAS", "IRACEMA", "IRAUCUBA", "ITAICABA", "ITAITINGA", 
    "ITAPAJE", "ITAPIPOCA", "ITAPIUNA", "ITAREMA", "ITATIRA", "JAGUARETAMA", "JAGUARIBARA", "JAGUARIBE", 
    "JAGUARUANA", "JARDIM", "JATI", "JIJOCA%20DE%20JERICOACOARA", "JUAZEIRO%20DO%20NORTE", "JUCAS", 
    "LAVRAS%20DA%20MANGABEIRA", "LIMOEIRAS%20DO%20NORTE", "MADALENA", "MARACANAU", "MARANGUAPE", "MARCO", 
    "MARTINOPOLE", "MASSAPE", "MAURITI", "MERUOCA", "MILAGRES", "MILHA", "MIRAIMA", "MISSAO%20VELHA", 
    "MOMBACA", "MONSENHOR%20TABOSA", "MORADA%20NOVA", "MORAUJO", "MORRINHOS", "MUCAMBO", "MULUNGU", 
    "NOVA%20OLINDA", "NOVA%20RUSSAS", "NOVO%20ORIENTE", "OCARA", "OROS", "PACAJUS", "PACATUBA", "PACOTI", 
    "PACUJA", "PALHANO", "PALMACIA", "PARACURU", "PARAIPABA", "PARAMBU", "PARAMOTI", "PEDRA%20BRANCA", 
    "PENAFORTE", "PENTECOSTE", "PEREIRO", "PINDORETAMA", "PIQUET%20CARNEIR", "PIRES%20FERREIRA", "PORANGA", 
    "PORTEIRAS", "POTENGI", "POTIRETAMA", "QUITERIANOPOLIS", "QUIXADA", "QUIXELO", "QUIXERAMOBIM", "QUIXERE", 
    "REDENCAO", "RERIUTABA", "RUSSAS", "SABOEIRO", "SALITRE", "SANTA%20QUITERIA", "SANTANA%20DO%20ACARAU", 
    "SANTANA%20DO%20CARIRI", "SAO%20BENEDITO", "SAO%20GONCALO%20DO%20AMARANTE", "SAO%20JOAO%20DO%20JAGUARIBE", 
    "SAO%20LUIS%20DO%20CURU", "SENADOR%20POMPEU", "SENADOR%20SA", "SOBRAL", "SOLONOPOLE", "TABULEIRAS%20DO%20NORTE", 
    "TAMBORIL", "TARRAFAS", "TAUA", "TEJUCUOCA", "TIANGUA", "TRAIRI", "TURURU", "UBAJARA", "UMARI", "UMIRIM", 
    "URUBURETAMA", "URUOCA", "VARJOTA", "VARZEA%20ALEGRE", "VICOSA%20DO%20CEARA"
]

# Estatísticas
stats = defaultdict(int, {
    'municipios_total': len(municipios),
    'municipios_processados': 0,
    'municipios_falha': 0,
    'municipios_sem_dados': 0,
    'municipios_com_erros': 0,
    'registros_processados': 0,
    'registros_invalidos': 0,
    'registros_json_recebidos': 0,
    'erros_comunicacao': 0,
    'registros_com_problemas': 0,
    'imoveis_com_problemas_por_municipio': defaultdict(int)
})

# Configurações
try:
    import config
    TOKEN_GEOAPI = config.settings.TOKEN_GEOAPI
except:
    TOKEN_GEOAPI = os.environ.get('TOKEN_GEOAPI')
    if not TOKEN_GEOAPI:
        raise ValueError("Token da GeoAPI não encontrado")


# Classe para gerenciar a interface ncurses
class ProgressMonitor:
    def __init__(self):
        self.stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.stdscr.keypad(True)
        self.progress_win = curses.newwin(10, 80, 0, 0)
        
    def update_progress(self, municipio_atual, total_processados, stats):
        self.progress_win.clear()
        self.progress_win.addstr(0, 0, "PROGRESSO DA COLETA DE DADOS")
        self.progress_win.addstr(1, 0, f"Município atual: {municipio_atual}")
        self.progress_win.addstr(2, 0, f"Processados: {total_processados}/{stats['municipios_total']}")
        
        # Barra de progresso
        progresso = int((total_processados/stats['municipios_total'])*50)
        barra = "[" + "#" * progresso + " " * (50 - progresso) + "]"
        self.progress_win.addstr(3, 0, barra)
        
        # Estatísticas
        self.progress_win.addstr(5, 0, "ESTATÍSTICAS:")
        self.progress_win.addstr(6, 0, f"Municípios com falha: {stats['municipios_falha']}")
        self.progress_win.addstr(7, 0, f"Erros de comunicação: {stats['erros_comunicacao']}")
        self.progress_win.addstr(8, 0, f"Registros processados: {stats['registros_processados']}")
        self.progress_win.addstr(9, 0, f"Registros JSON recebidos: {stats['registros_json_recebidos']}")
        
        self.progress_win.refresh()
        
    def close(self):
        curses.nocbreak()
        self.stdscr.keypad(False)
        curses.echo()
        curses.endwin()

# Cliente GeoAPI com retry
class GeoAPIClient:
    BASE_URL = "http://geoapi.idace.ce.gov.br/geoapi/pessoa/municipio/"
    
    def __init__(self):
        self.token = TOKEN_GEOAPI
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=lambda retry_state: logger.warning(
            f"Tentativa {retry_state.attempt_number} falhou. Retentando em {retry_state.next_action.sleep} segundos."
        )
    )
    def fetch_data(self, municipio, pagina=0, tamanho=10000):
        url = f"{self.BASE_URL}{municipio}?pagina={pagina}&tamanho={tamanho}&ordenarPor=proprietario"
        try:
            response = requests.get(url, headers=self.headers, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de comunicação com la API: {str(e)}")
            stats['erros_comunicacao'] += 1
            raise

# Função para normalizar nome do município
def normalizar_nome_municipio(nome):
    # Remove acentos e caracteres especiais
    nome_sem_acentos = unidecode.unidecode(nome)
    # Converte para minúsculo e substitui espaços por underscore
    nome_normalizado = nome_sem_acentos.lower().replace(' ', '_')
    return nome_normalizado

# Função para verificar padrões em texto
def verificar_padrao(texto, padroes):
    if not texto:
        return False
    
    texto = str(texto).lower()
    for padrao in padroes:
        if re.search(padrao, texto):
            return True
    return False

# Função principal para coleta de dados
def coletar_dados(progress_monitor):
    logger.info("Iniciando coleta de dados da GeoAPI")
    geo_client = GeoAPIClient()
    
    csv_filename = "datasets/malha_fundiaria_ceara_bruto.csv"
    fieldnames = [
        'lote_id', 'nome_municipio_original', 'nome_proprietario', 'imovel',
        'codigo_distrito', 'ponto_de_referencia', 'codigo_municipio', 
        'geometry_31984', 'centroide', 'nome_distrito', 'data_criacao_lote',
        'data_modificacao_lote', 'situacao_juridica', 'numero_incra',
        'numero_titulo', 'numero_lote'
    ]
    
    # Lista para armazenar registros com problemas
    registros_com_problemas = []
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for municipio in municipios:
            clean_name = municipio.replace("%20", " ")
            logger.info(f"Processando município: {clean_name}")
            
            # Atualiza display de progresso
            progress_monitor.update_progress(
                clean_name, 
                stats['municipios_processados'], 
                stats
            )
            
            try:
                data = geo_client.fetch_data(municipio)
                
                # Atualiza estatística de registros JSON recebidos
                if data and isinstance(data, list):
                    stats['registros_json_recebidos'] += len(data)
                
                if not data or not isinstance(data, list):
                    logger.warning(f"Nenhum dado válido para {clean_name}")
                    stats['municipios_sem_dados'] += 1
                    continue
                    
                problemas_municipio = 0
                
                for item in data:
                    try:
                        if not item.get('multipolygon'):
                            logger.warning(f"Registro {item.get('id')} sem geometria")
                            stats['registros_invalidos'] += 1
                            problemas_municipio += 1
                            
                            # Adiciona à lista de registros com problemas
                            registros_com_problemas.append({
                                'lote_id': item.get('loteId'),
                                'nome_municipio_original': item.get('municipio'),
                                'nome_proprietario': item.get('proprietario'),
                                'imovel': item.get('imovel'),
                                'codigo_distrito': item.get('codigoDistrito'),
                                'ponto_de_referencia': item.get('pontoDeReferencia'),
                                'codigo_municipio': item.get('codigoMunicipio'),
                                'geometry_31984': item.get('multipolygon'),
                                'centroide': item.get('centroide'),
                                'nome_distrito': item.get('nomeDistrito'),
                                'data_criacao_lote': item.get('dhc'),
                                'data_modificacao_lote': item.get('dhm'),
                                'situacao_juridica': item.get('situacaoJuridica'),
                                'numero_incra': item.get('sncr'),
                                'numero_titulo': item.get('titulo'),
                                'numero_lote': item.get('numero'),
                                'motivo': 'Sem geometria'
                            })
                            continue
                            
                        record = {
                            'lote_id': item.get('loteId'),
                            'nome_municipio_original': item.get('municipio'),
                            'nome_proprietario': item.get('proprietario'),
                            'imovel': item.get('imovel'),
                            'codigo_distrito': item.get('codigoDistrito'),
                            'ponto_de_referencia': item.get('pontoDeReferencia'),
                            'codigo_municipio': item.get('codigoMunicipio'),
                            'geometry_31984': item.get('multipolygon'),
                            'centroide': item.get('centroide'),
                            'nome_distrito': item.get('nomeDistrito'),
                            'data_criacao_lote': item.get('dhc'),
                            'data_modificacao_lote': item.get('dhm'),
                            'situacao_juridica': item.get('situacaoJuridica'),
                            'numero_incra': item.get('sncr'),
                            'numero_titulo': item.get('titulo'),
                            'numero_lote': item.get('numero')
                        }
                        
                        writer.writerow(record)
                        stats['registros_processados'] += 1
                        
                    except Exception as e:
                        logger.error(f"Erro no registro {item.get('id')}: {str(e)}")
                        stats['registros_invalidos'] += 1
                        problemas_municipio += 1
                        
                        # Adiciona à lista de registros com problemas
                        registros_com_problemas.append({
                            'lote_id': item.get('loteId'),
                            'nome_municipio_original': item.get('municipio'),
                            'nome_proprietario': item.get('proprietario'),
                            'imovel': item.get('imovel'),
                            'codigo_distrito': item.get('codigoDistrito'),
                            'ponto_de_referencia': item.get('pontoDeReferencia'),
                            'codigo_municipio': item.get('codigoMunicipio'),
                            'geometry_31984': item.get('multipolygon'),
                            'centroide': item.get('centroide'),
                            'nome_distrito': item.get('nomeDistrito'),
                            'data_criacao_lote': item.get('dhc'),
                            'data_modificacao_lote': item.get('dhm'),
                            'situacao_juridica': item.get('situacaoJuridica'),
                            'numero_incra': item.get('sncr'),
                            'numero_titulo': item.get('titulo'),
                            'numero_lote': item.get('numero'),
                            'motivo': f'Erro no processamento: {str(e)}'
                        })
                
                # Registra número de imóveis com problemas neste município
                stats['imoveis_com_problemas_por_municipio'][clean_name] = problemas_municipio
                logger.info(f"{clean_name}: {len(data)} registros processados, {problemas_municipio} com problemas")
                
            except Exception as e:
                logger.error(f"Erro em {clean_name}: {str(e)}")
                stats['municipios_com_erros'] += 1
                stats['municipios_falha'] += 1
            
            stats['municipios_processados'] += 1
    
    # Salva registros com problemas
    if registros_com_problemas:
        os.makedirs("para_averiguacao", exist_ok=True)
        data_hoje = datetime.now().strftime("%d%m%Y")
        problemas_filename = f"para_averiguacao/registros_com_problemas_{data_hoje}.csv"
        
        with open(problemas_filename, 'w', newline='', encoding='utf-8') as f:
            fieldnames_problemas = fieldnames + ['motivo']
            writer = csv.DictWriter(f, fieldnames=fieldnames_problemas)
            writer.writeheader()
            writer.writerows(registros_com_problemas)
        
        logger.info(f"Registros com problemas salvos em: {problemas_filename}")
        stats['registros_com_problemas'] = len(registros_com_problemas)
    
    logger.info(f"Coleta concluída. Arquivo {csv_filename} gerado.")
    return csv_filename

# Função para processar dados adicionais
def processar_dados_adicionais(csv_filename):
    logger.info("Iniciando processamento de dados adicionais")
    
    # Carrega dados do CSV bruto
    df = pd.read_csv(csv_filename, low_memory=False)
    
    # Carrega dados dos módulos fiscais
    modulos_df = pd.read_csv("datasets/regioes_municipios_modulos_fiscais_ceara.csv", low_memory=False)
    
    # 1. Normaliza nome do município
    df['nome_municipio'] = df['nome_municipio_original'].apply(normalizar_nome_municipio)
    
    # 2. Adiciona região administrativa
    df = df.merge(
        modulos_df[['nome_municipio', 'regiao_administrativa']], 
        on='nome_municipio', 
        how='left'
    )
    
    # 3. Adiciona módulo fiscal
    df = df.merge(
        modulos_df[['nome_municipio', 'modulo_fiscal']], 
        on='nome_municipio', 
        how='left'
    )
    
    # 4. Verifica se é quilombo
    padroes_quilombo = [r'quilomb']
    df['ehquilombo'] = df.apply(
        lambda x: verificar_padrao(x['nome_proprietario'], padroes_quilombo) or 
                 verificar_padrao(x['imovel'], padroes_quilombo), 
        axis=1
    )
    
    # 5. Verifica se é indígena
    padroes_indigena = [r'indígen', r'indigen']
    df['ehindigena'] = df.apply(
        lambda x: verificar_padrao(x['nome_proprietario'], padroes_indigena) or 
                 verificar_padrao(x['imovel'], padroes_indigena), 
        axis=1
    )
    
    # 6. Verifica se é assentamento
    padroes_assentamento = [r'assenta']
    df['ehassentamento'] = df.apply(
        lambda x: verificar_padrao(x['nome_proprietario'], padroes_assentamento) or 
                 verificar_padrao(x['imovel'], padroes_assentamento), 
        axis=1
    )
    
    # Identifica registros com problemas (sem geometry_31984 ou nulos)
    df_problemas = df[df['geometry_31984'].isna() | (df['geometry_31984'] == '')].copy()
    
    # Adiciona motivo do problema
    df_problemas['motivo'] = 'Geometry_31984 inválido ou nulo'
    
    # Remove registros com problemas do DataFrame principal
    df = df[df['geometry_31984'].notna() & (df['geometry_31984'] != '')]
    
    # Verifica se arquivo final já existe e renomeia se necessário
    output_filename = "datasets/malha_fundiaria_ceara.csv"
    if os.path.exists(output_filename):
        data_hoje = datetime.now().strftime("%d%m%Y")
        novo_nome = f"datasets/malha_fundiaria_ceara.csv.{data_hoje}"
        shutil.move(output_filename, novo_nome)
        logger.info(f"Arquivo existente renomeado para: {novo_nome}")
    
    # Salva o dataset final (sem as colunas de área, categoria e geometry)
    df.to_csv(output_filename, index=False, encoding='utf-8')
    
    # Salva registros com problemas se houver
    if not df_problemas.empty:
        os.makedirs("para_averiguacao", exist_ok=True)
        data_hoje = datetime.now().strftime("%d%m%Y")
        problemas_filename = f"para_averiguacao/registros_com_problemas_{data_hoje}.csv"
        
        # Se o arquivo já existe, vamos adicionar a ele
        if os.path.exists(problemas_filename):
            df_problemas.to_csv(problemas_filename, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df_problemas.to_csv(problemas_filename, index=False, encoding='utf-8')
        
        logger.info(f"Registros com problemas salvos em: {problemas_filename}")
    
    logger.info(f"Processamento concluído. Arquivo {output_filename} gerado.")
    return output_filename, len(df_problemas)

# Função para imprimir estatísticas
def print_stats(stats):
    print("\n=== RESUMO ESTATÍSTICO ===")
    print(f"• Municípios totais: {stats['municipios_total']}")
    print(f"• Municípios processados: {stats['municipios_processados']}")
    print(f"• Municípios com falha: {stats['municipios_falha']}")
    print(f"• Municípios sem dados: {stats['municipios_sem_dados']}")
    print(f"• Erros de comunicação: {stats['erros_comunicacao']}")
    print(f"• Registros JSON recebidos: {stats['registros_json_recebidos']}")
    print(f"• Registros processados: {stats['registros_processados']}")
    print(f"• Registros inválidos: {stats['registros_invalidos']}")
    print(f"• Registros com problemas: {stats['registros_com_problemas']}")
    
    print("\n• Imóveis com problemas por município:")
    for municipio, problemas in stats['imoveis_com_problemas_por_municipio'].items():
        print(f"  {municipio}: {problemas} problemas")
    
    print("=========================\n")

# Função principal
def main():
    # Inicializa monitor de progresso
    progress_monitor = ProgressMonitor()
    
    try:
        logger.info("Iniciando processo completo de importação e processamento")
        
        # Coleta dados da API e gera CSV bruto
        csv_bruto = coletar_dados(progress_monitor)
        
        # Fecha interface ncurses antes do processamento adicional
        progress_monitor.close()
        
        # Processa dados adicionais
        csv_processado, registros_com_problemas = processar_dados_adicionais(csv_bruto)
        stats['registros_com_problemas'] += registros_com_problemas
        
        # Exibe estatísticas finais
        print_stats(stats)
        logger.info("\n==== RESUMO FINAL ====")
        for key, value in stats.items():
            if key != 'imoveis_com_problemas_por_municipio':
                logger.info(f"{key}: {value}")
        
        # Log detalhado de problemas por município
        logger.info("Problemas por município:")
        for municipio, problemas in stats['imoveis_com_problemas_por_municipio'].items():
            logger.info(f"{municipio}: {problemas} problemas")
            
        logger.info("Processo concluído")
        
    except Exception as e:
        progress_monitor.close()
        logger.error(f"Erro no processo principal: {str(e)}")
        print(f"Erro: {str(e)}")
        sys.exit(1)
        
    except KeyboardInterrupt:
        progress_monitor.close()
        logger.info("Processo interrompido pelo usuário")
        print("Processo interrompido")
        sys.exit(0)

if __name__ == "__main__":
    main()