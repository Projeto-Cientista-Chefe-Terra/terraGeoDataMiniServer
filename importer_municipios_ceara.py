# importers/importer_municipios_ceara.py

import os
import logging
from datetime import datetime
import csv
import sys
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
import config

# =========================
# Logging
# =========================
log_filename = datetime.now().strftime("logs/importer_municipios_ceara_%Y_%m_%d_%H_%M.log")
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================
# Config
# =========================
settings = config.settings
TABLE_NAME = settings.TABLE_GEOM_MUNICIPIOS
CSV_PATH = os.path.join('datasets', 'municipios_ceara.csv')
SRID = getattr(settings, "GEOM_MUNICIPIOS_SRID", 4674)

engine = create_engine(settings.postgres_dsn)

# =========================
# Helpers
# =========================
def _is_nullish(value):
    if value is None:
        return True
    s = str(value).strip().lower()
    return s == "" or s in {"null", "none", "na", "n/a", "nan"}

def parse_float(value):
    if _is_nullish(value):
        return None
    try:
        s = str(value).strip().replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        logger.warning(f"Valor float inválido: {value!r}")
        return None

def sanitize_str(value):
    return (value or "").strip() or None

# =========================
# Schema / Table
# =========================
def create_table():
    """Cria a tabela de municípios com geometria no SRID especificado."""
    create_ext_postgis = text("CREATE EXTENSION IF NOT EXISTS postgis;")

    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            cd_mun       TEXT,
            nm_mun       TEXT,
            cd_rgi       TEXT,
            nm_rgi       TEXT,
            cd_rgint     TEXT,
            nm_rgint     TEXT,
            cd_uf        TEXT,
            nm_uf        TEXT,
            cd_regiao    TEXT,
            nm_regiao    TEXT,
            cd_concurb   TEXT,
            nm_concurb   TEXT,
            area_km2     DOUBLE PRECISION,
            geometry     GEOMETRY(MULTIPOLYGON, {SRID}),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    idx_cd_mun = DDL(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_cd_mun ON {TABLE_NAME}(cd_mun);")
    idx_nm_mun = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_nm_mun ON {TABLE_NAME}(nm_mun);")
    idx_geom = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geom ON {TABLE_NAME} USING GIST(geometry);")

    try:
        with engine.begin() as conn:
            try:
                conn.execute(create_ext_postgis)
            except SQLAlchemyError as e:
                logger.warning(f"Não foi possível garantir PostGIS: {e}")

            conn.execute(create_table_ddl)
            conn.execute(idx_cd_mun)
            conn.execute(idx_nm_mun)
            conn.execute(idx_geom)
            logger.info(f"Tabela e índices prontos: {TABLE_NAME}")
    except SQLAlchemyError as e:
        logger.error(f"Erro ao criar tabela/índices: {str(e)}")
        raise

# =========================
# Import
# =========================
def import_csv_data():
    """Importa os dados do CSV para a tabela, convertendo geometria hexadecimal para PostGIS."""
    stats = {"municipios": set(), "registros_salvos": 0}

    if not os.path.exists(CSV_PATH):
        logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
        return None

    try:
        # Aumentar limite de campo para lidar com geometrias grandes
        csv.field_size_limit(sys.maxsize)

        records = []
        with open(CSV_PATH, mode='r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            expected = {
                "cd_mun", "nm_mun", "cd_rgi", "nm_rgi", "cd_rgint", "nm_rgint", 
                "cd_uf", "nm_uf", "cd_regiao", "nm_regiao", "cd_concurb", 
                "nm_concurb", "area_km2", "geometry"
            }
            
            missing = [c for c in expected if c not in reader.fieldnames]
            if missing:
                logger.error(f"CSV ausente de colunas esperadas: {missing}")
                return None

            for row in reader:
                nm_mun = sanitize_str(row.get("nm_mun"))
                if nm_mun:
                    stats["municipios"].add(nm_mun)

                records.append({
                    "cd_mun": sanitize_str(row.get("cd_mun")),
                    "nm_mun": nm_mun,
                    "cd_rgi": sanitize_str(row.get("cd_rgi")),
                    "nm_rgi": sanitize_str(row.get("nm_rgi")),
                    "cd_rgint": sanitize_str(row.get("cd_rgint")),
                    "nm_rgint": sanitize_str(row.get("nm_rgint")),
                    "cd_uf": sanitize_str(row.get("cd_uf")),
                    "nm_uf": sanitize_str(row.get("nm_uf")),
                    "cd_regiao": sanitize_str(row.get("cd_regiao")),
                    "nm_regiao": sanitize_str(row.get("nm_regiao")),
                    "cd_concurb": sanitize_str(row.get("cd_concurb")),
                    "nm_concurb": sanitize_str(row.get("nm_concurb")),
                    "area_km2": parse_float(row.get("area_km2")),
                    "geometry": row.get("geometry")  # Geometria em formato hexadecimal
                })

        if not records:
            logger.info("CSV lido, mas sem registros.")
            return {"municipios": set(), "registros_salvos": 0, "qtd_municipios": 0}

        with engine.begin() as conn:
            # Inserir dados convertendo geometria hexadecimal para PostGIS
            insert_sql = text(f"""
                INSERT INTO {TABLE_NAME} (
                    cd_mun, nm_mun, cd_rgi, nm_rgi, cd_rgint, nm_rgint,
                    cd_uf, nm_uf, cd_regiao, nm_regiao, cd_concurb, nm_concurb,
                    area_km2, geometry
                ) VALUES (
                    :cd_mun, :nm_mun, :cd_rgi, :nm_rgi, :cd_rgint, :nm_rgint,
                    :cd_uf, :nm_uf, :cd_regiao, :nm_regiao, :cd_concurb, :nm_concurb,
                    :area_km2, ST_SetSRID(ST_GeomFromEWKB(decode(:geometry, 'hex')), {SRID})
                )
                ON CONFLICT (cd_mun) DO UPDATE SET
                    nm_mun = EXCLUDED.nm_mun,
                    cd_rgi = EXCLUDED.cd_rgi,
                    nm_rgi = EXCLUDED.nm_rgi,
                    cd_rgint = EXCLUDED.cd_rgint,
                    nm_rgint = EXCLUDED.nm_rgint,
                    cd_uf = EXCLUDED.cd_uf,
                    nm_uf = EXCLUDED.nm_uf,
                    cd_regiao = EXCLUDED.cd_regiao,
                    nm_regiao = EXCLUDED.nm_regiao,
                    cd_concurb = EXCLUDED.cd_concurb,
                    nm_concurb = EXCLUDED.nm_concurb,
                    area_km2 = EXCLUDED.area_km2,
                    geometry = EXCLUDED.geometry;
            """)
            
            conn.execute(insert_sql, records)
            stats["registros_salvos"] = len(records)
            logger.info(f"{len(records)} registros inseridos/atualizados em {TABLE_NAME}")

        stats["qtd_municipios"] = len(stats["municipios"])
        return stats

    except Exception as e:
        logger.error(f"Erro durante importação: {e}")
        return None

# =========================
# Main
# =========================
def main():
    logger.info("Iniciando importação de municípios do Ceará")

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
        print("\n=== RESUMO DA IMPORTAÇÃO (MUNICÍPIOS) ===")
        print(f"Municípios distintos: {stats['qtd_municipios']}")
        print(f"Registros inseridos/atualizados: {stats['registros_salvos']}")
        print(f"Tabela: {TABLE_NAME} | SRID: {SRID}")
        print("=========================================")
    else:
        logger.error("Importação falhou; verifique os logs.")

if __name__ == "__main__":
    main()