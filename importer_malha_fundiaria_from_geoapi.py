# importer_malha_fundiaria_from_geoapi.py

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

# Lista de municípios
municipios = ["IRAUCUBA"]

# Estatísticas
stats = defaultdict(int, {
    'municipios_total': len(municipios),
    'municipios_processados': 0,
    'municipios_sem_dados': 0,
    'municipios_com_erros': 0,
    'registros_brutos': 0,
    'registros_nao_dict': 0,
    'registros_sem_geometria': 0,
    'registros_sem_data_valida': 0,
    'registros_inseridos': 0,
    'registros_invalidos': 0,
    'erros_srid': 0,
    'registros_duplicatas_identicas': 0,
    'registros_inconsistentes': 0,
    'grupos_duplicatas_identicas': 0,
    'grupos_inconsistentes': 0
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

insert_query = text(f"""
    INSERT INTO {TABLE_NAME} (
        geoapi_id, lote_id, municipio, proprietario, imovel, 
        codigo_distrito, ponto_de_referencia, codigo_municipio, 
        multipolygon, centroide, nome_distrito, dhc, dhm, 
        situacao_juridica, sncr, titulo, numero
    ) VALUES (
        :geoapi_id, :lote_id, :municipio, :proprietario, :imovel, 
        :codigo_distrito, :ponto_de_referencia, :codigo_municipio, 
        ST_Transform(ST_SetSRID(ST_GeomFromEWKB(:multipolygon), 31984), 3857),
        ST_Transform(ST_SetSRID(ST_GeomFromEWKT(:centroide), 31984), 3857),
        :nome_distrito, to_timestamp(:dhc, 'YYYY-MM-DD HH24:MI:SS.US'), 
        to_timestamp(:dhm, 'YYYY-MM-DD HH24:MI:SS.US'), 
        :situacao_juridica, :sncr, :titulo, :numero
    )
    ON CONFLICT (geoapi_id) DO UPDATE SET
        lote_id = EXCLUDED.lote_id,
        municipio = EXCLUDED.municipio,
        proprietario = EXCLUDED.proprietario,
        imovel = EXCLUDED.imovel,
        codigo_distrito = EXCLUDED.codigo_distrito,
        ponto_de_referencia = EXCLUDED.ponto_de_referencia,
        codigo_municipio = EXCLUDED.codigo_municipio,
        multipolygon = EXCLUDED.multipolygon,
        centroide = EXCLUDED.centroide,
        nome_distrito = EXCLUDED.nome_distrito,
        dhc = EXCLUDED.dhc,
        dhm = EXCLUDED.dhm,
        situacao_juridica = EXCLUDED.situacao_juridica,
        sncr = EXCLUDED.sncr,
        titulo = EXCLUDED.titulo,
        numero = EXCLUDED.numero
""")

def prepare_data_for_logging(data):
    """Prepara dados para logging, convertendo bytes quando necessário"""
    if isinstance(data, bytes):
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return str(data)
    elif isinstance(data, dict):
        return {k: prepare_data_for_logging(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [prepare_data_for_logging(item) for item in data]
    return data

def create_table():
    """Cria a tabela sem o campo cpfcnpj"""
    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            geoapi_id BIGINT,
            lote_id BIGINT,
            municipio VARCHAR(100),
            proprietario VARCHAR(255),
            imovel VARCHAR(255),
            codigo_distrito BIGINT,
            ponto_de_referencia TEXT,
            codigo_municipio INTEGER,
            multipolygon GEOMETRY(MULTIPOLYGON, 3857),
            centroide GEOMETRY(POINT, 3857),
            nome_distrito VARCHAR(255),
            dhc TIMESTAMP,
            dhm TIMESTAMP,
            situacao_juridica VARCHAR(100),
            sncr VARCHAR(50),
            titulo VARCHAR(100),
            numero VARCHAR(50)
        );
        
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geoapi_id 
        ON {TABLE_NAME} (geoapi_id);
        
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_multipolygon 
        ON {TABLE_NAME} USING GIST (multipolygon);
        
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_centroide 
        ON {TABLE_NAME} USING GIST (centroide);
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(create_table_ddl)
            logger.info(f"Tabela {TABLE_NAME} criada/verificada")
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    except SQLAlchemyError as e:
        logger.error(f"Erro ao criar tabela: {str(e)}")
        raise

def remove_sensitive_fields(record):
    """Remove campos sensíveis de um registro"""
    record.pop('cpfcnpj', None)
    return record

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
    print(f"• Registros não-dicionários: {stats['registros_nao_dict']}")
    print(f"• Registros sem geometria: {stats['registros_sem_geometria']}")
    print(f"• Registros sem data válida: {stats['registros_sem_data_valida']}")
    print(f"• Registros salvos: {stats['registros_inseridos']}")
    print(f"• Registros inválidos: {stats['registros_invalidos']}")
    print(f"• Erros de SRID: {stats['erros_srid']}")
    print(f"• Registros duplicatas idênticas removidos: {stats['registros_duplicatas_identicas']}")
    print(f"• Grupos de duplicatas idênticas: {stats['grupos_duplicatas_identicas']}")
    print(f"• Registros inconsistentes removidos: {stats['registros_inconsistentes']}")
    print(f"• Grupos inconsistentes: {stats['grupos_inconsistentes']}")
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
                
            # Estrutura para armazenar dados brutos originais
            raw_records = []
            records = []
            non_dict_count = 0
            sem_geometria_count = 0
            sem_data_valida_count = 0
            other_invalid_count = 0
            
            for item in raw_data:
                try:
                    if item is None or not isinstance(item, dict):
                        # logger.debug(f"Registro não-dicionário ou Nulos encontrados: {type(item)} - {str(item)[:100]}")
                        non_dict_count += 1
                        continue
                        
                    if not item.get('multipolygon'):
                        # logger.debug(f"Registro sem geometria: {item.get('id', 'N/A')}")
                        sem_geometria_count += 1
                        continue
                        
                    # Armazenar dados brutos para possível exportação
                    raw_records.append(item)
                    
                    # Criar registro para inserção no banco (sem cpfcnpj)
                    record = {
                        'geoapi_id': item['id'],
                        'lote_id': item.get('lote_id'),
                        'municipio': item.get('municipio'),
                        'proprietario': item.get('proprietario'),
                        'imovel': item.get('imovel'),
                        'codigo_distrito': item.get('codigo_distrito'),
                        'ponto_de_referencia': item.get('ponto_de_referencia'),
                        'codigo_municipio': item.get('codigo_municipio'),
                        'multipolygon': bytes.fromhex(item['multipolygon']),
                        'centroide': item['centroide'],
                        'nome_distrito': item.get('nome_distrito'),
                        'dhc': safe_timestamp(item.get('dhc')),
                        'dhm': safe_timestamp(item.get('dhm')),
                        'situacao_juridica': item.get('situacao_juridica'),
                        'sncr': item.get('sncr'),
                        'titulo': item.get('titulo'),
                        'numero': item.get('numero')
                    }
                    
                    if not record['dhc'] or not record['dhm']:
                        sem_data_valida_count += 1
                        continue
                        
                    records.append(record)
                    
                except Exception as e:
                    logger.error(f"Erro no registro {item.get('id', 'N/A')}: {str(e)}")
                    other_invalid_count += 1
            
            stats['registros_nao_dict'] += non_dict_count
            stats['registros_sem_geometria'] += sem_geometria_count
            stats['registros_sem_data_valida'] += sem_data_valida_count
            stats['registros_invalidos'] += (non_dict_count + sem_geometria_count + sem_data_valida_count + other_invalid_count)
            
            logger.info(f"Detalhamento para {clean_name}:")
            logger.info(f" - Registros brutos: {len(raw_data)}")
            logger.info(f" - Não-dicionários: {non_dict_count}")
            logger.info(f" - Sem geometria: {sem_geometria_count}")
            logger.info(f" - Sem data válida: {sem_data_valida_count}")
            logger.info(f" - Outros inválidos: {other_invalid_count}")
            logger.info(f" - Válidos para inserção: {len(records)}")
            
            # Agrupar registros por geoapi_id usando dados brutos
            grupos_por_geoapi_id = defaultdict(list)
            for idx, item in enumerate(raw_records):
                grupos_por_geoapi_id[item['id']].append(idx)
            
            duplicatas_identicas = []
            inconsistencias = []
            indices_para_remover = set()
            
            # Processar grupos de registros com mesmo geoapi_id
            for geoapi_id, indices in grupos_por_geoapi_id.items():
                if len(indices) == 1:
                    continue
                
                # Campos para comparação (sem cpfcnpj)
                campos_chave = ['lote_id', 'numero', 'sncr', 'dhc', 'dhm']
                primeiro_idx = indices[0]
                todos_iguais = True
                
                # Verificar se todos os registros no grupo têm campos idênticos
                for idx in indices[1:]:
                    for campo in campos_chave:
                        if raw_records[idx].get(campo) != raw_records[primeiro_idx].get(campo):
                            todos_iguais = False
                            break
                    if not todos_iguais:
                        break
                
                if todos_iguais:
                    # Salvar grupo de duplicatas idênticas
                    grupo_duplicatas = [raw_records[idx] for idx in indices]
                    duplicatas_identicas.extend(grupo_duplicatas)
                    
                    # Marcar todos exceto o primeiro para remoção
                    indices_para_remover.update(indices[1:])
                    
                    stats['grupos_duplicatas_identicas'] += 1
                    stats['registros_duplicatas_identicas'] += len(indices) - 1
                else:
                    # Salvar grupo inconsistente
                    grupo_inconsistente = [raw_records[idx] for idx in indices]
                    inconsistencias.extend(grupo_inconsistente)
                    
                    # Marcar todos para remoção
                    indices_para_remover.update(indices)
                    
                    stats['grupos_inconsistentes'] += 1
                    stats['registros_inconsistentes'] += len(indices)
            
            # Criar lista final de registros para inserção
            final_records = [
                record 
                for idx, record in enumerate(records) 
                if idx not in indices_para_remover
            ]
            
            # Salvar objetos idênticos em arquivo (removendo cpfcnpj)
            if duplicatas_identicas:
                # Remover campo sensível antes de salvar
                duplicatas_sem_sensivel = [
                    remove_sensitive_fields(record.copy()) 
                    for record in duplicatas_identicas
                ]
                
                os.makedirs("para_averiguacao", exist_ok=True)
                data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"objetos_identicos_{clean_name}_{data_hora}.json"
                filepath = os.path.join("para_averiguacao", filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(duplicatas_sem_sensivel, f, ensure_ascii=False, indent=2)
                
                logger.info(f"Salvos {len(duplicatas_sem_sensivel)} registros duplicados idênticos em {filepath}")
            
            # Salvar registros inconsistentes se necessário (removendo cpfcnpj)
            if inconsistencias:
                # Remover campo sensível antes de salvar
                inconsistencias_sem_sensivel = [
                    remove_sensitive_fields(record.copy()) 
                    for record in inconsistencias
                ]
                
                os.makedirs("para_averiguacao", exist_ok=True)
                data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"objetos_inconsistentes_{clean_name}_{data_hora}.json"
                filepath = os.path.join("para_averiguacao", filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(inconsistencias_sem_sensivel, f, ensure_ascii=False, indent=2)
                
                logger.info(f"Salvos {len(inconsistencias_sem_sensivel)} registros inconsistentes em {filepath}")
            
            logger.info(f"Registros após remoção de duplicatas: {len(final_records)}")
            
            if final_records:
                try:
                    with engine.begin() as conn:
                        conn.execute(insert_query, final_records)
                        inserted = len(final_records)
                        stats['registros_inseridos'] += inserted
                        logger.info(f"{inserted} registros inseridos/atualizados")
                        
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