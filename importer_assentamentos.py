# importers/importer_assentamentos_ceara.py

import os
import logging
from datetime import datetime
import csv
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
import config

# Configuração de logging
log_filename = datetime.now().strftime("logs/importer_assentamentos_ceara_%Y_%m_%d_%H_%M.log")
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
TABLE_NAME = settings.TABLE_DADOS_ASSENTAMENTOS
CSV_PATH = os.path.join('datasets', 'assentamentos_ceara.csv') 

# Conexão com o banco de dados
engine = create_engine(settings.postgres_dsn)

def create_table():
    """Cria a tabela no banco de dados se não existir"""
    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            cd_sipra VARCHAR(255),
            nome_municipio VARCHAR(255),
            nome_municipio_original VARCHAR(255),
            nome_assentamento VARCHAR(255),
            area DOUBLE PRECISION,
            perimetro DOUBLE PRECISION,
            forma_obtecao VARCHAR(255),
            tipo_assentamento VARCHAR(255),
            num_familias INTEGER,
            wkt_geometry TEXT,
            geom GEOMETRY(MULTIPOLYGON, 4326),
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

def import_csv_data():
    """Importa dados do CSV para o banco de dados"""
    stats = {
        'municipios': set(),
        'assentamentos': set(),
        'registros_salvos': 0
    }
    
    try:
        # Ler arquivo CSV
        with open(CSV_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Preparar dados para inserção
            records = []
            for row in reader:
                cd_sipra = row.get('cd_sipra', '').strip()
                municipio = row.get('nome_municipio', '').strip()
                municipio_original = row.get('nome_municipio_original', '').strip()
                assentamento = row.get('nome_assentamento', '').strip()
                area = row.get('area', '0')
                perimetro = row.get('perimetro', '0')
                forma_obtecao = row.get('forma_obtecao', '').strip()
                tipo_assentamento = row.get('tipo_assentamento', '').strip()
                num_familias = row.get('num_familias', '0')
                wkt_geometry = row.get('wkt_geometry', '').strip()
                
                if municipio:
                    stats['municipios'].add(municipio)
                if assentamento:
                    stats['assentamentos'].add(assentamento)
                
                # Converter valores numéricos
                try:
                    area = float(area) if area else 0.0
                except ValueError:
                    area = 0.0
                    logger.warning(f"Valor inválido para área: {row.get('area')}")
                
                try:
                    perimetro = float(perimetro) if perimetro else 0.0
                except ValueError:
                    perimetro = 0.0
                    logger.warning(f"Valor inválido para perímetro: {row.get('perimetro')}")
                
                try:
                    num_familias_int = int(num_familias) if num_familias else None
                except ValueError:
                    num_familias_int = None
                    logger.warning(f"Valor inválido para número de famílias: {row.get('num_familias')}")
                
                records.append({
                    'cd_sipra': cd_sipra if cd_sipra != 'Null' else None,
                    'nome_municipio': municipio,
                    'nome_municipio_original': municipio_original,
                    'nome_assentamento': assentamento,
                    'area': area,
                    'perimetro': perimetro,
                    'forma_obtecao': forma_obtecao if forma_obtecao != 'Null' else None,
                    'tipo_assentamento': tipo_assentamento,
                    'num_familias': num_familias_int,
                    'wkt_geometry': wkt_geometry
                })
        
        # Inserir dados no banco
        if records:
            with engine.begin() as conn:
                # Primeiro inserir os dados básicos
                insert_query = text(f"""
                    INSERT INTO {TABLE_NAME} (
                        cd_sipra, nome_municipio, nome_municipio_original, 
                        nome_assentamento, area, perimetro, forma_obtecao, 
                        tipo_assentamento, num_familias, wkt_geometry
                    )
                    VALUES (
                        :cd_sipra, :nome_municipio, :nome_municipio_original,
                        :nome_assentamento, :area, :perimetro, :forma_obtecao,
                        :tipo_assentamento, :num_familias, :wkt_geometry
                    )
                """)
                
                # Corrigir o nome do parâmetro para nome_municipio_original
                corrected_records = []
                for record in records:
                    corrected_record = record.copy()
                    corrected_record['nome_municipio_original'] = corrected_record.pop('nome_municipio_original')
                    corrected_records.append(corrected_record)
                
                conn.execute(insert_query, corrected_records)
                
                # Atualizar a coluna geom a partir do WKT
                update_geom_query = text(f"""
                    UPDATE {TABLE_NAME} 
                    SET geom = ST_GeomFromText(wkt_geometry, 4326)
                    WHERE geom IS NULL
                """)
                conn.execute(update_geom_query)
                
                stats['registros_salvos'] = len(records)
                logger.info(f"{len(records)} registros inseridos com sucesso")
        
        # Converter sets para contagem
        stats['qtd_municipios'] = len(stats['municipios'])
        stats['qtd_assentamentos'] = len(stats['assentamentos'])
        
        return stats
        
    except FileNotFoundError:
        logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
        return None
    except Exception as e:
        logger.error(f"Erro durante importação: {str(e)}")
        return None

def main():
    logger.info("Iniciando importação de dados de assentamentos do Ceará")
    
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
        print(f"Quantidade de Municípios: {stats['qtd_municipios']}")
        print(f"Quantidade de Assentamentos: {stats['qtd_assentamentos']}")
        print(f"Quantidade de Registros Salvos: {stats['registros_salvos']}")
        print("===========================")
        
        logger.info("\n==== RESUMO FINAL ====")
        logger.info(f"Quantidade de Municípios: {stats['qtd_municipios']}")
        logger.info(f"Quantidade de Assentamentos: {stats['qtd_assentamentos']}")
        logger.info(f"Quantidade de Registros Salvos: {stats['registros_salvos']}")
    else:
        logger.error("Importação falhou, verifique os logs para detalhes")

if __name__ == "__main__":
    main()