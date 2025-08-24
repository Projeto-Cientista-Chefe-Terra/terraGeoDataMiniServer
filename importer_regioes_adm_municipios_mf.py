# importers/importer_regioes_adm_municipios_mf.py

import os
import logging
from datetime import datetime
import csv
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
import config

# Configuração de logging
log_filename = datetime.now().strftime("logs/importer_csv_rm_mun_mf_%Y_%m_%d_%H_%M.log")
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler()  # <- imprime na tela
    ],
    # force=True  # descomente se o script for importado/rodado mais de uma vez na mesma sessão
)
logger = logging.getLogger(__name__)

# Configurações
settings = config.settings
TABLE_NAME = settings.TABLE_RA_MUNICIPIOS_MF_CE
CSV_PATH = os.path.join('datasets', 'regioes_municipios_modulos_fiscais_ceara.csv') 

# Conexão com o banco de dados
engine = create_engine(settings.postgres_dsn)

def create_table():
    """Cria a tabela no banco de dados se não existir"""
    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            regiao_administrativa VARCHAR(255),
            nome_municipio VARCHAR(255),
            modulo_fiscal DOUBLE PRECISION,
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
        'regioes_administrativas': set(),
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
                regiao = row.get('regiao_administrativa', '').strip()
                municipio = row.get('nome_municipio', '').strip()
                modulo_fiscal = row.get('modulo_fiscal', '')
                
                if regiao:
                    stats['regioes_administrativas'].add(regiao)
                if municipio:
                    stats['municipios'].add(municipio)
                
                records.append({
                    'regiao_administrativa': regiao,
                    'nome_municipio': municipio,
                    'modulo_fiscal': modulo_fiscal
                })
        
        # Inserir dados no banco
        if records:
            with engine.begin() as conn:
                insert_query = text(f"""
                    INSERT INTO {TABLE_NAME} (regiao_administrativa, nome_municipio,modulo_fiscal)
                    VALUES (:regiao_administrativa, :nome_municipio, :modulo_fiscal)
                """)
                conn.execute(insert_query, records)
                stats['registros_salvos'] = len(records)
                logger.info(f"{len(records)} registros inseridos com sucesso")
        
        # Converter sets para contagem
        stats['qtd_regioes'] = len(stats['regioes_administrativas'])
        stats['qtd_municipios'] = len(stats['municipios'])
        
        return stats
        
    except FileNotFoundError:
        logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
        return None
    except Exception as e:
        logger.error(f"Erro durante importação: {str(e)}")
        return None

def main():
    logger.info("Iniciando importação de dados do CSV")
    
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
        print(f"Quantidade de Regiões Administrativas: {stats['qtd_regioes']}")
        print(f"Quantidade de Municípios: {stats['qtd_municipios']}")
        print(f"Quantidade de Registros Salvos: {stats['registros_salvos']}")
        print("===========================")
        
        logger.info("\n==== RESUMO FINAL ====")
        logger.info(f"Quantidade de Regiões Administrativas: {stats['qtd_regioes']}")
        logger.info(f"Quantidade de Municípios: {stats['qtd_municipios']}")
        logger.info(f"Quantidade de Registros Salvos: {stats['registros_salvos']}")
    else:
        logger.error("Importação falhou, verifique os logs para detalhes")

if __name__ == "__main__":
    main()