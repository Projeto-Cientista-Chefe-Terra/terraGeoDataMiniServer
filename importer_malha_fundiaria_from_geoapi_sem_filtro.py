# importer_malha_fundiaria_from_geoapi_sem_filtro.py

import os
import logging
from datetime import datetime
from collections import defaultdict
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
from geoalchemy2 import Geometry
import config
import json
from functools import partial

# Configuração de logging
log_filename = datetime.now().strftime("logs/geoapi_importer_%Y_%m_%d_%H_%M.log")
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Lista de municípios (mantida igual)
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
    "LAVRAS%20DA%20MANGABEIRA", "LIMOEIRO%20DO%20NORTE", "MADALENA", "MARACANAU", "MARANGUAPE", "MARCO",
    "MARTINOPOLE", "MASSAPE", "MAURITI", "MERUOCA", "MILAGRES", "MILHA", "MIRAIMA", "MISSAO%20VELHA",
    "MOMBACA", "MONSENHOR%20TABOSA", "MORADA%20NOVA", "MORAUJO", "MORRINHOS", "MUCAMBO", "MULUNGU",
    "NOVA%20OLINDA", "NOVA%20RUSSAS", "NOVO%20ORIENTE", "OCARA", "OROS", "PACAJUS", "PACATUBA", "PACOTI",
    "PACUJA", "PALHANO", "PALMACIA", "PARACURU", "PARAIPABA", "PARAMBU", "PARAMOTI", "PEDRA%20BRANCA",
    "PENAFORTE", "PENTECOSTE", "PEREIRO", "PINDORETAMA", "PIQUET%20CARNEIRO", "PIRES%20FERREIRA", "PORANGA",
    "PORTEIRAS", "POTENGI", "POTIRETAMA", "QUITERIANOPOLIS", "QUIXADA", "QUIXELO", "QUIXERAMOBIM", "QUIXERE",
    "REDENCAO", "RERIUTABA", "RUSSAS", "SABOEIRO", "SALITRE", "SANTA%20QUITERIA", "SANTANA%20DO%20ACARAU",
    "SANTANA%20DO%20CARIRI", "SAO%20BENEDITO", "SAO%20GONCALO%20DO%20AMARANTE", "SAO%20JOAO%20DO%20JAGUARIBE",
    "SAO%20LUIS%20DO%20CURU", "SENADOR%20POMPEU", "SENADOR%20SA", "SOBRAL", "SOLONOPOLE", "TABULEIRO%20DO%20NORTE",
    "TAMBORIL", "TARRAFAS", "TAUA", "TEJUCUOCA", "TIANGUA", "TRAIRI", "TURURU", "UBAJARA", "UMARI", "UMIRIM",
    "URUBURETAMA", "URUOCA", "VARJOTA", "VARZEA%20ALEGRE", "VICOSA%20DO%20CEARA"
]

# Estatísticas simplificadas
stats = defaultdict(int, {
    'municipios_total': len(municipios),
    'municipios_processados': 0,
    'municipios_sem_dados': 0,
    'municipios_com_erros': 0,
    'registros_brutos': 0,
    'registros_sem_geometria': 0,
    'registros_inseridos': 0,
    'registros_invalidos': 0,
    'erros_srid': 0
})

# Configurações
settings = config.settings
TABLE_NAME = settings.TABLE_TEMPORARY

# Conexão com o banco de dados
engine = create_engine(settings.postgres_dsn)

def safe_timestamp(ts_str):
    """Converte strings de timestamp de forma segura"""
    if not ts_str:
        return None
    try:
        if '.' in ts_str:
            parts = ts_str.split('.')
            if len(parts) == 2 and len(parts[1]) > 6:
                ts_str = f"{parts[0]}.{parts[1][:6]}"
        return ts_str
    except Exception:
        return None

def create_table():
    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            geoapi_id BIGINT,
            lote_id BIGINT,
            nome_municipio VARCHAR(100),
            nome_proprietario VARCHAR(255),
            imovel VARCHAR(255),
            codigo_distrito BIGINT,
            ponto_de_referencia TEXT,
            codigo_municipio INTEGER,
            geometry GEOMETRY(MULTIPOLYGON, 3857),
            centroide GEOMETRY(POINT, 3857),
            nome_distrito VARCHAR(255),
            data_criacao TIMESTAMP,
            data_modificacao TIMESTAMP,
            situacao_juridica VARCHAR(100),
            numero_incra VARCHAR(50),
            numero_titulo VARCHAR(100),
            numero_lote VARCHAR(50)
        );
        
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geoapi_id ON {TABLE_NAME} (geoapi_id);
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geometry ON {TABLE_NAME} USING GIST (geometry);
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_centroide ON {TABLE_NAME} USING GIST (centroide);
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(create_table_ddl)
            logger.info(f"Tabela {TABLE_NAME} criada/verificada")
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    except SQLAlchemyError as e:
        logger.error(f"Erro ao criar tabela: {str(e)}")
        raise

insert_query = text(f"""
    INSERT INTO {TABLE_NAME} (
        geoapi_id, lote_id, nome_municipio, nome_proprietario, imovel, 
        codigo_distrito, ponto_de_referencia, codigo_municipio, 
        geometry, centroide, nome_distrito, data_criacao, data_modificacao, 
        situacao_juridica, numero_incra, numero_titulo, numero_lote
    ) VALUES (
        :geoapi_id, :lote_id, :nome_municipio, :nome_proprietario, :imovel, 
        :codigo_distrito, :ponto_de_referencia, :codigo_municipio, 
        ST_Transform(ST_SetSRID(ST_GeomFromEWKB(:geometry), 31984), 3857),
        ST_Transform(ST_SetSRID(ST_GeomFromEWKT(:centroide), 31984), 3857),
        :nome_distrito, to_timestamp(:data_criacao, 'YYYY-MM-DD HH24:MI:SS.US'), 
        to_timestamp(:data_modificacao, 'YYYY-MM-DD HH24:MI:SS.US'), 
        :situacao_juridica, :numero_incra, :numero_titulo, :numero_lote
    )
""")

def save_records_without_geometry(records, municipio):
    """Salva registros sem geometria em arquivo JSON"""
    if not records:
        return
    
    os.makedirs("para_averiguar", exist_ok=True)
    data_hora = datetime.now().strftime("%Y_%m_%d_%H_%M")
    filename = f"sem_geometria_{data_hora}.json"
    filepath = os.path.join("para_averiguar", filename)
    
    # Remove campos sensíveis antes de salvar
    records_to_save = []
    for record in records:
        clean_record = record.copy()
        clean_record.pop('cpfcnpj', None)
        records_to_save.append(clean_record)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(records_to_save, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Salvos {len(records_to_save)} registros sem geometria em {filepath}")

class GeoAPIClient:
    BASE_URL = "http://geoapi.idace.ce.gov.br/geoapi/pessoa/municipio/"
    
    def __init__(self):
        self.token = settings.TOKEN_GEOAPI
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def fetch_data(self, municipio, pagina=0, tamanho=10000):
        url = f"{self.BASE_URL}{municipio}?pagina={pagina}&tamanho={tamanho}&ordenarPor=proprietario"
        try:
            response = requests.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if data and isinstance(data, list):
                logger.debug(f"Total de registros recebidos: {len(data)}")
                return data
            return []
            
        except Exception as e:
            logger.error(f"Erro na requisição para {municipio}: {str(e)}")
            raise

def print_stats(stats):
    """Exibe estatísticas formatadas no terminal"""
    print("\n=== RESUMO ESTATÍSTICO ===")
    print(f"• Municípios totais: {stats['municipios_total']}")
    print(f"• Municípios processados: {stats['municipios_processados']}")
    print(f"• Municípios sem dados: {stats['municipios_sem_dados']}")
    print(f"• Municípios com erros: {stats['municipios_com_erros']}")
    print(f"• Total de registros brutos: {stats['registros_brutos']}")
    print(f"• Registros sem geometria: {stats['registros_sem_geometria']}")
    print(f"• Registros salvos: {stats['registros_inseridos']}")
    print(f"• Registros inválidos: {stats['registros_invalidos']}")
    print(f"• Erros de SRID: {stats['erros_srid']}")
    print("=========================\n")

def main():
    logger.info("Iniciando importação de dados da GeoAPI")
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexão com o PostgreSQL OK")
    except Exception as e:
        logger.error(f"Falha na conexão com PostgreSQL: {str(e)}")
        raise
    
    geo_client = GeoAPIClient()
    create_table()
    
    for municipio in municipios:
        clean_name = municipio.replace("%20", " ")
        logger.info(f"\nProcessando município: {clean_name}")
        stats['municipios_processados'] += 1
        
        try:
            raw_data = geo_client.fetch_data(municipio)
            stats['registros_brutos'] += len(raw_data)
            
            if not isinstance(raw_data, list):
                logger.error(f"Dados inválidos para {clean_name} - tipo: {type(raw_data)}")
                stats['municipios_com_erros'] += 1
                continue
                
            if len(raw_data) == 0:
                logger.warning(f"Nenhum dado encontrado para {clean_name}")
                stats['municipios_sem_dados'] += 1
                continue
                
            records_with_geometry = []
            records_without_geometry = []
            
            for item in raw_data:
                try:
                    if item is None or not isinstance(item, dict):
                        stats['registros_invalidos'] += 1
                        continue
                        
                    # Criar registro com os novos nomes de campos
                    record = {
                        'geoapi_id': item.get('id'),
                        'lote_id': item.get('lote_id'),
                        'nome_municipio': item.get('municipio'),
                        'nome_proprietario': item.get('proprietario'),
                        'imovel': item.get('imovel'),
                        'codigo_distrito': item.get('codigo_distrito'),
                        'ponto_de_referencia': item.get('ponto_de_referencia'),
                        'codigo_municipio': item.get('codigo_municipio'),
                        'geometry': bytes.fromhex(item['multipolygon']) if item.get('multipolygon') else None,
                        'centroide': item.get('centroide'),
                        'nome_distrito': item.get('nome_distrito'),
                        'data_criacao': safe_timestamp(item.get('dhc')),
                        'data_modificacao': safe_timestamp(item.get('dhm')),
                        'situacao_juridica': item.get('situacao_juridica'),
                        'numero_incra': item.get('sncr'),
                        'numero_titulo': item.get('titulo'),
                        'numero_lote': item.get('numero')
                    }
                    
                    if record['geometry']:
                        records_with_geometry.append(record)
                    else:
                        records_without_geometry.append(item)  # Salvar o item original
                        stats['registros_sem_geometria'] += 1
                        
                except Exception as e:
                    logger.error(f"Erro no registro {item.get('id', 'N/A')}: {str(e)}")
                    stats['registros_invalidos'] += 1
            
            # Salvar registros sem geometria
            if records_without_geometry:
                save_records_without_geometry(records_without_geometry, clean_name)
            
            # Inserir registros com geometria
            if records_with_geometry:
                try:
                    with engine.begin() as conn:
                        conn.execute(insert_query, records_with_geometry)
                        inserted = len(records_with_geometry)
                        stats['registros_inseridos'] += inserted
                        logger.info(f"{inserted} registros inseridos")
                        
                except SQLAlchemyError as e:
                    if "SRID" in str(e):
                        stats['erros_srid'] += 1
                    logger.error(f"Falha ao inserir registros: {str(e)}")
                    stats['municipios_com_erros'] += 1
                    
        except Exception as e:
            logger.error(f"Erro em {clean_name}: {str(e)}", exc_info=True)
            stats['municipios_com_erros'] += 1
    
    try:
        with engine.connect() as conn:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar()
            logger.info(f"Total final no banco: {total}")
    except Exception as e:
        logger.error(f"Erro ao verificar dados inseridos: {str(e)}")
    
    print_stats(stats)
    logger.info("\n==== RESUMO FINAL ====")
    for key, value in stats.items():
        logger.info(f"{key}: {value}")
    logger.info("Processo concluído")

if __name__ == "__main__":
    main()