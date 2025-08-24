# importers/importer_reservatorios_ceara.py

import os
import logging
from datetime import datetime
import csv
import sys
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
import config

# Configuração de logging
log_filename = datetime.now().strftime("logs/importer_reservatorios_ceara_%Y_%m_%d_%H_%M.log")
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)

# Configurações
settings = config.settings
TABLE_NAME = settings.TABLE_DADOS_RESERVATORIOS
CSV_PATH = os.path.join('datasets', 'reservatorios_ceara.csv') 

# Conexão com o banco de dados
engine = create_engine(settings.postgres_dsn)

# Helpers
def _is_nullish(v):
    if v is None: return True
    s = str(v).strip().lower()
    return s == "" or s in {"null", "none", "na", "n/a", "nan"}

def parse_float(v):
    if _is_nullish(v): return None
    try:
        s = str(v).strip().replace(" ", "")
        if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
        elif "," in s:            s = s.replace(",", ".")
        return float(s)
    except Exception:
        logger.warning(f"Valor float inválido: {v!r}")
        return None

def parse_int(v):
    if _is_nullish(v): return None
    try:
        return int(float(str(v).replace(",", ".")))
    except Exception:
        logger.warning(f"Valor int inválido: {v!r}")
        return None

def sanitize_str(v):
    return (None if _is_nullish(v) else str(v).strip())

def create_table():
    """Cria a tabela no banco de dados se não existir"""
    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            wkt_geom TEXT,
            id_sagreh BIGINT,
            nome TEXT,
            proprietario TEXT,
            gerencia TEXT,
            reg_hidrog TEXT,
            ini_monito TEXT,
            ano_constr BIGINT,
            ri TEXT,
            o_barrad TEXT,
            ac_jusante BIGINT,
            id_ac_jus DOUBLE PRECISION,
            area_ha DOUBLE PRECISION,
            capacid_m3 DOUBLE PRECISION,
            cot_vert_m DOUBLE PRECISION,
            lg_vert_m DOUBLE PRECISION,
            cot_td_m TEXT,
            tipo_verte DOUBLE PRECISION,
            x DOUBLE PRECISION,
            y DOUBLE PRECISION,
            nome_municipio_original TEXT,
            nome_municipio TEXT,
            geometry GEOMETRY(MULTIPOLYGON, 4326),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    try:
        with engine.begin() as conn:
            # Garantir que a extensão PostGIS está instalada
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.execute(create_table_ddl)
            logger.info(f"Tabela {TABLE_NAME} criada/verificada")
    except SQLAlchemyError as e:
        logger.error(f"Erro ao criar tabela: {str(e)}")
        raise

def parse_date(date_str):
    """Tenta converter uma string para data no formato YYYY-MM-DD"""
    if not date_str or _is_nullish(date_str):
        return None
    try:
        # Assumindo que a data pode estar em diferentes formatos
        if '/' in date_str:
            day, month, year = date_str.split('/')
            return f"{year}-{month}-{day}"
        elif '-' in date_str:
            return date_str  # Já está no formato esperado
        else:
            # Se for apenas o ano
            return f"{date_str}-01-01"
    except Exception as e:
        logger.warning(f"Data inválida: {date_str} - {str(e)}")
        return None

def import_csv_data():
    """Importa dados do CSV para o banco de dados"""
    stats = {
        'reservatorios': set(),
        'municipios': set(),
        'registros_salvos': 0
    }
    
    try:
        # Aumentar o limite de campo para lidar com geometrias grandes
        csv.field_size_limit(sys.maxsize)
        
        # Ler arquivo CSV
        with open(CSV_PATH, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            
            # Preparar dados para inserção
            records = []
            for row in reader:
                nome = sanitize_str(row.get('nome'))
                municipio = sanitize_str(row.get('nome_municipio'))
                
                if nome:
                    stats['reservatorios'].add(nome)
                if municipio:
                    stats['municipios'].add(municipio)
                
                # Preparar registro
                record = {
                    'wkt_geom': sanitize_str(row.get('wkt_geom')),
                    'id_sagreh': parse_int(row.get('id_sagreh')),
                    'nome': nome,
                    'proprietario': sanitize_str(row.get('proprietario')),
                    'gerencia': sanitize_str(row.get('gerencia')),
                    'reg_hidrog': sanitize_str(row.get('reg_hidrog')),
                    'ini_monito': parse_date(sanitize_str(row.get('ini_monito'))),
                    'ano_constr': parse_int(row.get('ano_constr')),
                    'ri': sanitize_str(row.get('ri')),
                    'o_barrad': sanitize_str(row.get('o_barrad')),
                    'ac_jusante': parse_int(row.get('ac_jusante')),
                    'id_ac_jus': parse_float(row.get('id_ac_jus')),
                    'area_ha': parse_float(row.get('area_ha')),
                    'capacid_m3': parse_float(row.get('capacid_m3')),
                    'cot_vert_m': parse_float(row.get('cot_vert_m')),
                    'lg_vert_m': parse_float(row.get('lg_vert_m')),
                    'cot_td_m': sanitize_str(row.get('cot_td_m')),
                    'tipo_verte': parse_float(row.get('tipo_verte')),
                    'x': parse_float(row.get('x')),
                    'y': parse_float(row.get('y')),
                    'nome_municipio_original': sanitize_str(row.get('nome_municipio_original')),
                    'nome_municipio': municipio,
                    'geometry': row.get('geometry')  # Campo binário em formato hexadecimal
                }
                
                records.append(record)
        
        # Inserir dados no banco
        if records:
            with engine.begin() as conn:
                # Primeiro inserir os dados básicos
                insert_query = text(f"""
                    INSERT INTO {TABLE_NAME} (
                        wkt_geom, id_sagreh, nome, proprietario, gerencia, reg_hidrog,
                        ini_monito, ano_constr, ri, o_barrad, ac_jusante, id_ac_jus,
                        area_ha, capacid_m3, cot_vert_m, lg_vert_m, cot_td_m, tipo_verte,
                        x, y, nome_municipio_original, nome_municipio, geometry
                    )
                    VALUES (
                        :wkt_geom, :id_sagreh, :nome, :proprietario, :gerencia, :reg_hidrog,
                        :ini_monito, :ano_constr, :ri, :o_barrad, :ac_jusante, :id_ac_jus,
                        :area_ha, :capacid_m3, :cot_vert_m, :lg_vert_m, :cot_td_m, :tipo_verte,
                        :x, :y, :nome_municipio_original, :nome_municipio, :geometry
                    )
                """)
                
                conn.execute(insert_query, records)
                

                # for record in records:
                #     if record['geometry'] and not _is_nullish(record['geometry']):
                #         update_geom_query = text(f"""
                #             UPDATE {TABLE_NAME} 
                #             SET geom = ST_Multi(ST_GeomFromEWKB(decode(:geometry, 'hex')))
                #             WHERE nome = :nome AND nome_municipio = :municipio
                #         """)
                #         conn.execute(update_geom_query, {
                #             'geometry': record['geometry'],
                #             'nome': record['nome'],
                #             'municipio': record['nome_municipio']
                #         })
                
                # # Alternativamente, usar o WKT se o geometry não estiver disponível
                # update_geom_wkt_query = text(f"""
                #     UPDATE {TABLE_NAME} 
                #     SET geom = ST_GeomFromText(wkt_geom, 4326)
                #     WHERE geom IS NULL AND wkt_geom IS NOT NULL AND wkt_geom != ''
                # """)
                # conn.execute(update_geom_wkt_query)
                
                stats['registros_salvos'] = len(records)
                logger.info(f"{len(records)} registros inseridos com sucesso")
        
        # Converter sets para contagem
        stats['qtd_reservatorios'] = len(stats['reservatorios'])
        stats['qtd_municipios'] = len(stats['municipios'])
        
        return stats
        
    except FileNotFoundError:
        logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
        return None
    except Exception as e:
        logger.error(f"Erro durante importação: {str(e)}")
        return None

def main():
    logger.info("Iniciando importação de dados de reservatórios do Ceará")
    
    # Verificar conexão com o banco
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexão com o PostgreSQL OK")
    except Exception as e:
        logger.error(f"Falha na conexão com PostgreSQL: {str(e)}")
        return
    
    # Criar tabela
    create_table()
    
    # Importar dados
    stats = import_csv_data()
    
    # Exibir resumo
    if stats:
        print("\n=== RESUMO DA IMPORTAÇÃO ===")
        print(f"Quantidade de Reservatórios: {stats['qtd_reservatorios']}")
        print(f"Quantidade de Municípios: {stats['qtd_municipios']}")
        print(f"Quantidade de Registros Salvos: {stats['registros_salvos']}")
        print("===========================")
        
        logger.info("\n==== RESUMO FINAL ====")
        logger.info(f"Quantidade de Reservatórios: {stats['qtd_reservatorios']}")
        logger.info(f"Quantidade de Municípios: {stats['qtd_municipios']}")
        logger.info(f"Quantidade de Registros Salvos: {stats['registros_salvos']}")
    else:
        logger.error("Importação falhou, verifique os logs para detalhes")

if __name__ == "__main__":
    main()