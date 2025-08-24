# importers/importer_malha_fundiaria_ceara.py

import os
import logging
from datetime import datetime
import csv
import sys
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
from config import settings

# =========================
# Logging
# =========================
log_filename = datetime.now().strftime("logs/importer_malha_fundiaria_ceara_%Y_%m_%d_%H_%M.log")
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


# =========================
# Config
# =========================
TABLE_NAME = settings.TABLE_DADOS_FUNDIARIOS
CSV_PATH = os.path.join('datasets', 'malha_fundiaria_ceara.csv')

engine = create_engine(settings.postgres_dsn)

# =========================
# Helpers
# =========================
def _is_nullish(value):
    if value is None:
        return True
    s = str(value).strip().lower()
    return s == "" or s in {"null", "none", "na", "n/a", "nan"}

def parse_int(value):
    if _is_nullish(value):
        return None
    try:
        s = str(value).strip().replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except Exception:
        logger.warning(f"Valor inteiro inválido: {value!r}")
        return None

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

def parse_timestamp(value):
    if _is_nullish(value):
        return None
    s = str(value).strip()
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    logger.warning(f"Data inválida/inesperada: {value!r}")
    return None

def parse_bool(value):
    if _is_nullish(value):
        return None
    s = str(value).strip().lower()
    return s in {"t", "true", "1", "y", "yes", "s", "sim"}

def sanitize_str(value):
    return (value or "").strip() or None

# =========================
# Schema / Table
# =========================
def create_table():
    """
    Cria a tabela. 'geometry' é TEXT (EPSG:4326); 'geometry_31984' é GEOMETRY(MULTIPOLYGON, 31984).
    Sem colunas/atualizações derivadas.
    """
    create_ext_postgis = text("CREATE EXTENSION IF NOT EXISTS postgis;")

    create_table_ddl = DDL(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            lote_id BIGINT,
            pessoa_id DOUBLE PRECISION,
            nome_municipio TEXT,
            nome_proprietario TEXT,
            imovel TEXT,
            nome_distrito TEXT,
            codigo_distrito DOUBLE PRECISION,
            ponto_de_referencia TEXT,
            data_criacao_lote TIMESTAMP NULL,
            codigo_municipio DOUBLE PRECISION,
            geometry_31984 GEOMETRY(MULTIPOLYGON, 31984), -- armazenada como GEOMETRY 31984
            centroide TEXT,                                
            data_modificacao_lote TIMESTAMP NULL,
            situacao_juridica TEXT,
            numero_incra TEXT,
            numero_titulo TEXT,
            numero_lote TEXT,
            nome_municipio_original TEXT,
            regiao_administrativa TEXT,
            modulo_fiscal INTEGER,
            area DOUBLE PRECISION,
            categoria TEXT,
            geometry TEXT,                                  -- sempre TEXT (EPSG:4326) 
            ehquilombo BOOLEAN,
            ehindigena BOOLEAN,
            ehassentamento BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    idx_lote = DDL(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_lote_id ON {TABLE_NAME}(lote_id);")
    idx_mun  = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_nm_municipio ON {TABLE_NAME}(nome_municipio);")
    idx_gist = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geometry_31984 ON {TABLE_NAME} USING GIST(geometry_31984);")

    try:
        with engine.begin() as conn:
            try:
                conn.execute(create_ext_postgis)
            except SQLAlchemyError as e:
                logger.warning(f"Não foi possível garantir PostGIS: {e}")

            conn.execute(create_table_ddl)
            conn.execute(idx_lote)
            conn.execute(idx_mun)
            conn.execute(idx_gist)
            logger.info(f"Tabela e índices prontos: {TABLE_NAME}")
    except SQLAlchemyError as e:
        logger.error(f"Erro ao criar tabela/índices: {str(e)}")
        raise

# =========================
# Import
# =========================
def import_csv_data():
    """
    Importa datasets/malha_fundiaria_ceara.csv para settings.TABLE_DADOS_FUNDIARIOS.

    Colunas esperadas:
      lote_id,pessoa_id,nome_municipio,nome_proprietario,imovel,nome_distrito,codigo_distrito,
      ponto_de_referencia,data_criacao_lote,codigo_municipio,geometry_31984,centroide,
      data_modificacao_lote,situacao_juridica,numero_incra,numero_titulo,numero_lote,
      nome_municipio_original,regiao_administrativa,modulo_fiscal,area,categoria,geometry,
      ehquilombo,ehindigena,ehassentamento
    """
    stats = {"municipios": set(), "registros_salvos": 0}

    if not os.path.exists(CSV_PATH):
        logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
        return None

    try:
        # Aumentar o limite do campo CSV para evitar "field larger than field limit"
        csv.field_size_limit(sys.maxsize)

        records = []
        with open(CSV_PATH, mode='r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            expected = {
                "lote_id","pessoa_id","nome_municipio","nome_proprietario","imovel","nome_distrito",
                "codigo_distrito","ponto_de_referencia","data_criacao_lote","codigo_municipio",
                "geometry_31984","centroide","data_modificacao_lote","situacao_juridica","numero_incra",
                "numero_titulo","numero_lote","nome_municipio_original","regiao_administrativa",
                "modulo_fiscal","area","categoria","geometry","ehquilombo","ehindigena","ehassentamento"
            }
            missing = [c for c in expected if c not in reader.fieldnames]
            if missing:
                logger.error(f"CSV ausente de colunas esperadas: {missing}")
                return None

            for row in reader:
                nm = sanitize_str(row.get("nome_municipio"))
                if nm:
                    stats["municipios"].add(nm)

                records.append({
                    "lote_id": parse_int(row.get("lote_id")),  # Convertido para int
                    "pessoa_id": parse_float(row.get("pessoa_id")),  # Convertido para float
                    "nome_municipio": nm,
                    "nome_proprietario": sanitize_str(row.get("nome_proprietario")),
                    "imovel": sanitize_str(row.get("imovel")),
                    "nome_distrito": sanitize_str(row.get("nome_distrito")),
                    "codigo_distrito": parse_float(row.get("codigo_distrito")),  # Convertido para float
                    "ponto_de_referencia": sanitize_str(row.get("ponto_de_referencia")),
                    "data_criacao_lote": parse_timestamp(row.get("data_criacao_lote")),
                    "codigo_municipio": parse_float(row.get("codigo_municipio")),  # Convertido para float
                    "geometry_31984": row.get("geometry_31984"),  # HEX EWKB (assumido)
                    "centroide": sanitize_str(row.get("centroide")),            # WKT (texto)
                    "data_modificacao_lote": parse_timestamp(row.get("data_modificacao_lote")),
                    "situacao_juridica": sanitize_str(row.get("situacao_juridica")),
                    "numero_incra": sanitize_str(row.get("numero_incra")),
                    "numero_titulo": sanitize_str(row.get("numero_titulo")),
                    "numero_lote": row.get("numero_lote"),
                    "nome_municipio_original": sanitize_str(row.get("nome_municipio_original")),
                    "regiao_administrativa": sanitize_str(row.get("regiao_administrativa")),
                    "modulo_fiscal": parse_int(row.get("modulo_fiscal")),  # Convertido para int
                    "area": parse_float(row.get("area")),  # Convertido para float
                    "categoria": sanitize_str(row.get("categoria")),
                    "geometry": row.get("geometry"),              # TEXT 4326
                    "ehquilombo": parse_bool(row.get("ehquilombo")),
                    "ehindigena": parse_bool(row.get("ehindigena")),
                    "ehassentamento": parse_bool(row.get("ehassentamento")),
                })

        if not records:
            logger.info("CSV lido, mas sem registros.")
            return {"municipios": set(), "registros_salvos": 0, "qtd_municipios": 0}

        with engine.begin() as conn:
            # INSERT + UPSERT por lote_id
            insert_sql = text(f"""
                INSERT INTO {TABLE_NAME} (
                    lote_id, pessoa_id, nome_municipio, nome_proprietario, imovel, nome_distrito,
                    codigo_distrito, ponto_de_referencia, data_criacao_lote, codigo_municipio,
                    geometry_31984, centroide, data_modificacao_lote, situacao_juridica, numero_incra,
                    numero_titulo, numero_lote, nome_municipio_original, regiao_administrativa,
                    modulo_fiscal, area, categoria, geometry, ehquilombo, ehindigena, ehassentamento
                ) VALUES (
                    :lote_id, :pessoa_id, :nome_municipio, :nome_proprietario, :imovel, :nome_distrito,
                    :codigo_distrito, :ponto_de_referencia, :data_criacao_lote, :codigo_municipio,
                    CASE
                        WHEN :geometry_31984 IS NULL OR :geometry_31984 = '' THEN NULL
                        ELSE ST_Multi(ST_GeomFromEWKB(decode(:geometry_31984, 'hex')))
                    END,
                    :centroide, :data_modificacao_lote, :situacao_juridica, :numero_incra,
                    :numero_titulo, :numero_lote, :nome_municipio_original, :regiao_administrativa,
                    :modulo_fiscal, :area, :categoria, :geometry, :ehquilombo, :ehindigena, :ehassentamento
                )
                ON CONFLICT (lote_id) DO UPDATE SET
                    pessoa_id = EXCLUDED.pessoa_id,
                    nome_municipio = EXCLUDED.nome_municipio,
                    nome_proprietario = EXCLUDED.nome_proprietario,
                    imovel = EXCLUDED.imovel,
                    nome_distrito = EXCLUDED.nome_distrito,
                    codigo_distrito = EXCLUDED.codigo_distrito,
                    ponto_de_referencia = EXCLUDED.ponto_de_referencia,
                    data_criacao_lote = EXCLUDED.data_criacao_lote,
                    codigo_municipio = EXCLUDED.codigo_municipio,
                    geometry_31984 = EXCLUDED.geometry_31984,
                    centroide = EXCLUDED.centroide,
                    data_modificacao_lote = EXCLUDED.data_modificacao_lote,
                    situacao_juridica = EXCLUDED.situacao_juridica,
                    numero_incra = EXCLUDED.numero_incra,
                    numero_titulo = EXCLUDED.numero_titulo,
                    numero_lote = EXCLUDED.numero_lote,
                    nome_municipio_original = EXCLUDED.nome_municipio_original,
                    regiao_administrativa = EXCLUDED.regiao_administrativa,
                    modulo_fiscal = EXCLUDED.modulo_fiscal,
                    area = EXCLUDED.area,
                    categoria = EXCLUDED.categoria,
                    geometry = EXCLUDED.geometry,
                    ehquilombo = EXCLUDED.ehquilombo,
                    ehindigena = EXCLUDED.ehindigena,
                    ehassentamento = EXCLUDED.ehassentamento;
            """)
            conn.execute(insert_sql, records)

            stats["registros_salvos"] = len(records)
            logger.info(f"{len(records)} registros inseridos/atualizados em {TABLE_NAME}")

        stats["qtd_municipios"] = len(stats["municipios"])
        return stats

    except csv.Error as e:
        logger.error(f"Erro de leitura CSV (tamanho do campo?): {e}")
        return None
    except Exception as e:
        logger.error(f"Erro durante importação: {e}")
        return None

# =========================
# Main
# =========================
def main():
    logger.info("Iniciando importação da malha fundiária do Ceará")

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
        print("\n=== RESUMO DA IMPORTAÇÃO (MALHA FUNDIÁRIA) ===")
        print(f"Municípios distintos: {stats.get('qtd_municipios', 0)}")
        print(f"Registros inseridos/atualizados: {stats.get('registros_salvos', 0)}")
        print(f"Tabela: {TABLE_NAME}")
        print("==============================================")
    else:
        logger.error("Importação falhou; verifique os logs para detalhes")

if __name__ == "__main__":
    main()