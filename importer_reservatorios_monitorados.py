# importers/importer_reservatorios_ceara.py

import os
import logging
from datetime import datetime
import csv
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
import config

# Configuração de logging
log_filename = datetime.now().strftime("logs/importer_reservatorios_ceara_%Y_%m_%d_%H_%M.log")
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configurações
settings = config.settings
TABLE_NAME = settings.TABLE_DADOS_RESERVATORIOS
CSV_PATH = os.path.join('datasets', 'reservatorios_ceara.csv') 

# Conexão com o banco de dados
engine = create_engine(settings.postgres_dsn)

def create_table():
    """Cria a tabela no banco de dados se não existir"""
    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            wkt_geom TEXT,
            id_sagreh VARCHAR(255),
            nome VARCHAR(255),
            proprietario VARCHAR(255),
            gerencia VARCHAR(255),
            reg_hidrog VARCHAR(255),
            ini_monito DATE,
            ano_constr INTEGER,
            ri VARCHAR(255),
            o_barrad VARCHAR(255),
            ac_jusante VARCHAR(255),
            id_ac_jus VARCHAR(255),
            area_ha DOUBLE PRECISION,
            capacid_m3 DOUBLE PRECISION,
            cot_vert_m DOUBLE PRECISION,
            lg_vert_m DOUBLE PRECISION,
            cot_td_m DOUBLE PRECISION,
            tipo_verte VARCHAR(255),
            x DOUBLE PRECISION,
            y DOUBLE PRECISION,
            nome_municipio_original VARCHAR(255),
            nome_municipio VARCHAR(255),
            geom GEOMETRY(POINT, 4326),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(create_table_ddl)
            logger.info(f"Tabela {TABLE_NAME} criada/verificada")
    except SQLAlchemyError as e:
        logger.error(f"Erro ao criar tabela: {str(e)}")
        raise

def parse_date(date_str):
    """Tenta converter uma string para data no formato YYYY-MM-DD"""
    if not date_str or date_str.lower() == 'null':
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
        # Ler arquivo CSV
        with open(CSV_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Preparar dados para inserção
            records = []
            for row in reader:
                nome = row.get('nome', '').strip()
                municipio = row.get('nome_municipio', '').strip()
                
                if nome:
                    stats['reservatorios'].add(nome)
                if municipio:
                    stats['municipios'].add(municipio)
                
                # Tratar campos numéricos
                def parse_float(value):
                    try:
                        return float(value) if value and value.lower() != 'null' else None
                    except ValueError:
                        return None
                
                def parse_int(value):
                    try:
                        return int(value) if value and value.lower() != 'null' else None
                    except ValueError:
                        return None
                
                # Preparar registro
                record = {
                    'wkt_geom': row.get('wkt_geom', '').strip(),
                    'id_sagreh': row.get('id_sagreh', '').strip(),
                    'nome': nome,
                    'proprietario': row.get('proprietario', '').strip(),
                    'gerencia': row.get('gerencia', '').strip(),
                    'reg_hidrog': row.get('reg_hidrog', '').strip(),
                    'ini_monito': parse_date(row.get('ini_monito', '').strip()),
                    'ano_constr': parse_int(row.get('ano_constr', '')),
                    'ri': row.get('ri', '').strip(),
                    'o_barrad': row.get('o_barrad', '').strip(),
                    'ac_jusante': row.get('ac_jusante', '').strip(),
                    'id_ac_jus': row.get('id_ac_jus', '').strip(),
                    'area_ha': parse_float(row.get('area_ha', '')),
                    'capacid_m3': parse_float(row.get('capacid_m3', '')),
                    'cot_vert_m': parse_float(row.get('cot_vert_m', '')),
                    'lg_vert_m': parse_float(row.get('lg_vert_m', '')),
                    'cot_td_m': parse_float(row.get('cot_td_m', '')),
                    'tipo_verte': row.get('tipo_verte', '').strip(),
                    'x': parse_float(row.get('x', '')),
                    'y': parse_float(row.get('y', '')),
                    'nome_municipio_original': row.get('nome_municipio_original', '').strip(),
                    'nome_municipio': municipio
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
                        x, y, nome_municipio_original, nome_municipio
                    )
                    VALUES (
                        :wkt_geom, :id_sagreh, :nome, :proprietario, :gerencia, :reg_hidrog,
                        :ini_monito, :ano_constr, :ri, :o_barrad, :ac_jusante, :id_ac_jus,
                        :area_ha, :capacid_m3, :cot_vert_m, :lg_vert_m, :cot_td_m, :tipo_verte,
                        :x, :y, :nome_municipio_original, :nome_municipio
                    )
                """)
                
                conn.execute(insert_query, records)
                
                # Atualizar a coluna geom a partir das coordenadas X e Y
                update_geom_query = text(f"""
                    UPDATE {TABLE_NAME} 
                    SET geom = ST_SetSRID(ST_MakePoint(x, y), 4326)
                    WHERE geom IS NULL AND x IS NOT NULL AND y IS NOT NULL
                """)
                conn.execute(update_geom_query)
                
                # Alternativamente, poderia usar o WKT se disponível
                if any(r['wkt_geom'] for r in records):
                    update_geom_wkt_query = text(f"""
                        UPDATE {TABLE_NAME} 
                        SET geom = ST_GeomFromText(wkt_geom, 4326)
                        WHERE geom IS NULL AND wkt_geom IS NOT NULL AND wkt_geom != ''
                    """)
                    conn.execute(update_geom_wkt_query)
                
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