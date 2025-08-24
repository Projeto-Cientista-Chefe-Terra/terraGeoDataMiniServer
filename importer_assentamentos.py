# importers/importer_assentamentos_ceara.py

import os
import logging
from datetime import datetime
import csv
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
import config

# Logging
log_filename = datetime.now().strftime("logs/importer_assentamentos_ceara_%Y_%m_%d_%H_%M.log")
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

# Config
settings = config.settings
TABLE_NAME = settings.TABLE_DADOS_ASSENTAMENTOS
CSV_PATH = os.path.join('datasets', 'assentamentos_ceara.csv')

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

# DDL
def create_table():
    """
    Cria a tabela apenas com os campos do CSV de assentamentos.
    """
    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            cd_sipra VARCHAR(10),
            nome_municipio VARCHAR(100),
            nome_municipio_original VARCHAR(100),
            nome_assentamento VARCHAR(255),
            area DOUBLE PRECISION,
            perimetro DOUBLE PRECISION,
            forma_obtecao VARCHAR(255),
            tipo_assentamento VARCHAR(10),
            num_familias INTEGER,
            wkt_geometry TEXT,
            geom GEOMETRY(MULTIPOLYGON, 4326),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    idx_cd_sipra = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_cd_sipra ON {TABLE_NAME}(cd_sipra);")
    idx_mun      = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_mun ON {TABLE_NAME}(nome_municipio);")
    idx_geom     = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geom ON {TABLE_NAME} USING GIST(geom);")
    
    try:
        with engine.begin() as conn:
            # Garantir que a extensão PostGIS está instalada
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.execute(create_table_ddl)
            conn.execute(idx_cd_sipra)
            conn.execute(idx_mun)
            conn.execute(idx_geom)
            logger.info(f"Tabela {TABLE_NAME} criada/verificada")
    except SQLAlchemyError as e:
        logger.error(f"Erro ao criar tabela: {str(e)}")
        raise

def import_csv_data():
    """
    Importa dado SEM geometria do CSV:
    id,cd_sipra,nome_municipio,nome_municipio_original,nome_assentamento,
    area,perimetro,forma_obtecao,tipo_assentamento,num_familias
    """
    stats = {
        'municipios': set(), 
        'registros_unicos': set(),
        'registros_salvos': 0,
        'registros_duplicados': 0,
        'duplicatas_detalhes': []  # Nova lista para armazenar detalhes das duplicatas
    }

    expected = [
        "cd_sipra", "nome_municipio", "nome_municipio_original",
        "nome_assentamento", "area", "perimetro", "forma_obtecao",
        "tipo_assentamento", "num_familias", "wkt_geometry"
    ]

    try:
        if not os.path.exists(CSV_PATH):
            logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
            return None

        records = []
        chaves_vistas = set()  # Para armazenar chaves únicas (cd_sipra + nome_municipio)
        
        with open(CSV_PATH, mode='r', encoding='utf-8-sig', newline='') as file:
            reader = csv.DictReader(file)
            missing = [c for c in expected if c not in reader.fieldnames]
            if missing:
                logger.error(f"CSV ausente de colunas esperadas: {missing}")
                return None

            for row_num, row in enumerate(reader, 1):
                cd_sipra = sanitize_str(row.get('cd_sipra'))
                municipio = sanitize_str(row.get('nome_municipio'))
                municipio_original = sanitize_str(row.get('nome_municipio_original'))
                assent = sanitize_str(row.get('nome_assentamento'))
                area = parse_float(row.get('area'))
                perimetro = parse_float(row.get('perimetro'))
                familias = parse_int(row.get('num_familias'))

                # Usar chave composta para identificar registros únicos (CD_SIPRA + Município)
                chave_registro = (cd_sipra, municipio or municipio_original)
                
                if municipio:
                    stats['municipios'].add(municipio)
                
                if chave_registro in chaves_vistas:
                    stats['registros_duplicados'] += 1
                    # Armazenar detalhes da duplicata
                    stats['duplicatas_detalhes'].append({
                        'linha': row_num,
                        'cd_sipra': cd_sipra,
                        'municipio': municipio or municipio_original,
                        'assentamento': assent,
                        'dados': {k: v for k, v in row.items() if k in expected}  # Apenas campos esperados
                    })
                    logger.debug(f"Registro duplicado na linha {row_num}: {chave_registro}")
                    # Não adicionar este registro à lista de registros a serem inseridos
                    continue
                elif chave_registro[0] and chave_registro[1]:
                    stats['registros_unicos'].add(chave_registro)
                    chaves_vistas.add(chave_registro)

                records.append({
                    'cd_sipra': cd_sipra,
                    'nome_municipio': municipio,
                    'nome_municipio_original': municipio_original,
                    'nome_assentamento': assent,
                    'area': area if area is not None else 0.0,
                    'perimetro': perimetro if perimetro is not None else 0.0,
                    'forma_obtecao': sanitize_str(row.get('forma_obtecao')),
                    'tipo_assentamento': sanitize_str(row.get('tipo_assentamento')),
                    'num_familias': familias,
                    'wkt_geometry': sanitize_str(row.get('wkt_geometry'))
                })

        if records:
            with engine.begin() as conn:
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
                conn.execute(insert_query, records)
                stats['registros_salvos'] = len(records)
                logger.info(f"{len(records)} registros inseridos com sucesso")
                
                # Atualizar a coluna geom a partir do wkt_geometry
                update_geom_query = text(f"""
                    UPDATE {TABLE_NAME} 
                    SET geom = ST_Multi(ST_GeomFromText(wkt_geometry, 4326))
                    WHERE wkt_geometry IS NOT NULL 
                    AND wkt_geometry != ''
                    AND geom IS NULL;
                """)
                result = conn.execute(update_geom_query)
                logger.info(f"Geometrias convertidas: {result.rowcount} registros atualizados")

        stats['qtd_municipios'] = len(stats['municipios'])
        stats['qtd_registros_unicos'] = len(stats['registros_unicos'])
        return stats

    except Exception as e:
        logger.error(f"Erro durante importação: {e}")
        return None

def main():
    logger.info("Iniciando importação de dados de assentamentos do Ceará")

    # Teste de conexão
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexão com o PostgreSQL OK")
    except Exception as e:
        logger.error(f"Falha na conexão com PostgreSQL: {str(e)}")
        return

    create_table()
    stats = import_csv_data()

    if stats:
        print("\n=== RESUMO DA IMPORTAÇÃO ===")
        print(f"Quantidade de Municípios: {stats['qtd_municipios']}")
        print(f"Quantidade de Registros Únicos (CD_SIPRA + Município): {stats['qtd_registros_unicos']}")
        print(f"Registros Salvos: {stats['registros_salvos']}")
        print(f"Registros Duplicados Ignorados: {stats.get('registros_duplicados', 0)}")
        print("===========================")
        
        # Exibir detalhes das duplicatas se houver
        if stats.get('registros_duplicados', 0) > 0:
            logger.info(f"Detectados {stats['registros_duplicados']} registros duplicados que foram ignorados:")
            for duplicata in stats['duplicatas_detalhes']:
                logger.info(f"Linha {duplicata['linha']}: CD_SIPRA={duplicata['cd_sipra']}, Município={duplicata['municipio']}, Assentamento={duplicata['assentamento']}")
        
        # Verificar se os números fazem sentido
        total_registros = stats['registros_salvos'] + stats.get('registros_duplicados', 0)
        logger.info(f"Total de registros processados: {total_registros}")
        
        if stats['registros_salvos'] != stats['qtd_registros_unicos']:
            logger.warning(f"Discrepância detectada: Registros salvos ({stats['registros_salvos']}) != Registros únicos ({stats['qtd_registros_unicos']})")
    else:
        logger.error("Importação falhou, verifique os logs para detalhes")

if __name__ == "__main__":
    main()