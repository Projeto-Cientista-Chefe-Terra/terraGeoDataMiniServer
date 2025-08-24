# importers/importer_all.py

import os
import logging
from datetime import datetime, timedelta
import csv
import sys
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.exc import SQLAlchemyError
import config

# Configuração de logging
log_filename = datetime.now().strftime("logs/importer_unificado_%Y_%m_%d_%H_%M.log")
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

# Configurações
settings = config.settings
engine = create_engine(settings.postgres_dsn)

# Helper functions
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

def check_recent_data(table_name, hours=24):
    """Verifica se há dados recentes na tabela (últimas X horas)"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 FROM {table_name} 
                    WHERE created_at >= NOW() - INTERVAL '{hours} hours'
                    LIMIT 1
                )
            """))
            return result.scalar()
    except Exception as e:
        logger.warning(f"Erro ao verificar dados recentes em {table_name}: {e}")
        return False

def table_exists(table_name):
    """Verifica se a tabela existe no banco de dados"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name = '{table_name}'
                )
            """))
            return result.scalar()
    except Exception as e:
        logger.error(f"Erro ao verificar existência da tabela {table_name}: {e}")
        return False

def should_import_data(table_name, hours=24):
    """Determina se os dados devem ser importados com base na existência da tabela e dados recentes"""
    if not table_exists(table_name):
        logger.info(f"Tabela {table_name} não existe. Importação necessária.")
        return True
    
    if check_recent_data(table_name, hours):
        logger.info(f"Dados recentes encontrados em {table_name}. Importação não necessária.")
        return False
    
    logger.info(f"Dados em {table_name} são antigos. Importação necessária.")
    return True

# Importador de Assentamentos
def import_assentamentos():
    """Importa dados de assentamentos do Ceará"""
    TABLE_NAME = settings.TABLE_DADOS_ASSENTAMENTOS
    CSV_PATH = os.path.join('datasets', 'assentamentos_ceara.csv')
    
    if not should_import_data(TABLE_NAME):
        return {"status": "skipped", "reason": "Dados recentes existem"}
    
    def create_table():
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
        idx_mun = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_mun ON {TABLE_NAME}(nome_municipio);")
        idx_geom = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geom ON {TABLE_NAME} USING GIST(geom);")
        
        try:
            with engine.begin() as conn:
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
        stats = {
            'municipios': set(), 
            'registros_unicos': set(),
            'registros_salvos': 0,
            'registros_duplicados': 0,
            'duplicatas_detalhes': []
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
            chaves_vistas = set()
            
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

                    chave_registro = (cd_sipra, municipio or municipio_original)
                    
                    if municipio:
                        stats['municipios'].add(municipio)
                    
                    if chave_registro in chaves_vistas:
                        stats['registros_duplicados'] += 1
                        stats['duplicatas_detalhes'].append({
                            'linha': row_num,
                            'cd_sipra': cd_sipra,
                            'municipio': municipio or municipio_original,
                            'assentamento': assent,
                            'dados': {k: v for k, v in row.items() if k in expected}
                        })
                        logger.debug(f"Registro duplicado na linha {row_num}: {chave_registro}")
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

    try:
        create_table()
        stats = import_csv_data()
        return {"status": "success", "stats": stats} if stats else {"status": "error", "message": "Falha na importação"}
    except Exception as e:
        logger.error(f"Erro no processamento de assentamentos: {e}")
        return {"status": "error", "message": str(e)}

# Importador de Malha Fundiária
def import_malha_fundiaria():
    """Importa dados da malha fundiária do Ceará"""
    TABLE_NAME = settings.TABLE_DADOS_FUNDIARIOS
    CSV_PATH = os.path.join('datasets', 'malha_fundiaria_ceara.csv')
    
    if not should_import_data(TABLE_NAME):
        return {"status": "skipped", "reason": "Dados recentes existem"}
    
    def create_table():
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
                geometry_31984 GEOMETRY(MULTIPOLYGON, 31984),
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
                geometry TEXT,
                ehquilombo BOOLEAN,
                ehindigena BOOLEAN,
                ehassentamento BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        idx_lote = DDL(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_lote_id ON {TABLE_NAME}(lote_id);")
        idx_mun = DDL(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_nm_municipio ON {TABLE_NAME}(nome_municipio);")
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

    def import_csv_data():
        stats = {"municipios": set(), "registros_salvos": 0}

        if not os.path.exists(CSV_PATH):
            logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
            return None

        try:
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
                        "lote_id": parse_int(row.get("lote_id")),
                        "pessoa_id": parse_float(row.get("pessoa_id")),
                        "nome_municipio": nm,
                        "nome_proprietario": sanitize_str(row.get("nome_proprietario")),
                        "imovel": sanitize_str(row.get("imovel")),
                        "nome_distrito": sanitize_str(row.get("nome_distroit")),
                        "codigo_distrito": parse_float(row.get("codigo_distrito")),
                        "ponto_de_referencia": sanitize_str(row.get("ponto_de_referencia")),
                        "data_criacao_lote": parse_timestamp(row.get("data_criacao_lote")),
                        "codigo_municipio": parse_float(row.get("codigo_municipio")),
                        "geometry_31984": row.get("geometry_31984"),
                        "centroide": sanitize_str(row.get("centroide")),
                        "data_modificacao_lote": parse_timestamp(row.get("data_modificacao_lote")),
                        "situacao_juridica": sanitize_str(row.get("situacao_juridica")),
                        "numero_incra": sanitize_str(row.get("numero_incra")),
                        "numero_titulo": sanitize_str(row.get("numero_titulo")),
                        "numero_lote": row.get("numero_lote"),
                        "nome_municipio_original": sanitize_str(row.get("nome_municipio_original")),
                        "regiao_administrativa": sanitize_str(row.get("regiao_administrativa")),
                        "modulo_fiscal": parse_int(row.get("modulo_fiscal")),
                        "area": parse_float(row.get("area")),
                        "categoria": sanitize_str(row.get("categoria")),
                        "geometry": row.get("geometry"),
                        "ehquilombo": parse_bool(row.get("ehquilombo")),
                        "ehindigena": parse_bool(row.get("ehindigena")),
                        "ehassentamento": parse_bool(row.get("ehassentamento")),
                    })

            if not records:
                logger.info("CSV lido, mas sem registros.")
                return {"municipios": set(), "registros_salvos": 0, "qtd_municipios": 0}

            with engine.begin() as conn:
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

    try:
        create_table()
        stats = import_csv_data()
        return {"status": "success", "stats": stats} if stats else {"status": "error", "message": "Falha na importação"}
    except Exception as e:
        logger.error(f"Erro no processamento da malha fundiária: {e}")
        return {"status": "error", "message": str(e)}

# Importador de Municípios
def import_municipios():
    """Importa dados de municípios do Ceará"""
    TABLE_NAME = settings.TABLE_GEOM_MUNICIPIOS
    CSV_PATH = os.path.join('datasets', 'municipios_ceara.csv')
    SRID = getattr(settings, "GEOM_MUNICIPIOS_SRID", 4674)
    
    if not should_import_data(TABLE_NAME):
        return {"status": "skipped", "reason": "Dados recentes existem"}
    
    def create_table():
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

    def import_csv_data():
        stats = {"municipios": set(), "registros_salvos": 0}

        if not os.path.exists(CSV_PATH):
            logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
            return None

        try:
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
                        "geometry": row.get("geometry")
                    })

            if not records:
                logger.info("CSV lido, mas sem registros.")
                return {"municipios": set(), "registros_salvos": 0, "qtd_municipios": 0}

            with engine.begin() as conn:
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

    try:
        create_table()
        stats = import_csv_data()
        return {"status": "success", "stats": stats} if stats else {"status": "error", "message": "Falha na importação"}
    except Exception as e:
        logger.error(f"Erro no processamento de municípios: {e}")
        return {"status": "error", "message": str(e)}

# Importador de Regiões Administrativas
def import_regioes_adm():
    """Importa dados de regiões administrativas e municípios"""
    TABLE_NAME = settings.TABLE_RA_MUNICIPIOS_MF_CE
    CSV_PATH = os.path.join('datasets', 'regioes_municipios_modulos_fiscais_ceara.csv')
    
    if not should_import_data(TABLE_NAME):
        return {"status": "skipped", "reason": "Dados recentes existem"}
    
    def create_table():
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
        stats = {
            'regioes_administrativas': set(),
            'municipios': set(),
            'registros_salvos': 0
        }
        
        try:
            with open(CSV_PATH, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
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
            
            if records:
                with engine.begin() as conn:
                    insert_query = text(f"""
                        INSERT INTO {TABLE_NAME} (regiao_administrativa, nome_municipio, modulo_fiscal)
                        VALUES (:regiao_administrativa, :nome_municipio, :modulo_fiscal)
                    """)
                    conn.execute(insert_query, records)
                    stats['registros_salvos'] = len(records)
                    logger.info(f"{len(records)} registros inseridos com sucesso")
            
            stats['qtd_regioes'] = len(stats['regioes_administrativas'])
            stats['qtd_municipios'] = len(stats['municipios'])
            
            return stats
            
        except FileNotFoundError:
            logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
            return None
        except Exception as e:
            logger.error(f"Erro durante importação: {str(e)}")
            return None

    try:
        create_table()
        stats = import_csv_data()
        return {"status": "success", "stats": stats} if stats else {"status": "error", "message": "Falha na importação"}
    except Exception as e:
        logger.error(f"Erro no processamento de regiões administrativas: {e}")
        return {"status": "error", "message": str(e)}

# Importador de Reservatórios
def import_reservatorios():
    """Importa dados de reservatórios do Ceará"""
    TABLE_NAME = settings.TABLE_DADOS_RESERVATORIOS
    CSV_PATH = os.path.join('datasets', 'reservatorios_ceara.csv')
    
    if not should_import_data(TABLE_NAME):
        return {"status": "skipped", "reason": "Dados recentes existem"}
    
    def create_table():
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
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                conn.execute(create_table_ddl)
                logger.info(f"Tabela {TABLE_NAME} criada/verificada")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao criar tabela: {str(e)}")
            raise

    def parse_date(date_str):
        if not date_str or _is_nullish(date_str):
            return None
        try:
            if '/' in date_str:
                day, month, year = date_str.split('/')
                return f"{year}-{month}-{day}"
            elif '-' in date_str:
                return date_str
            else:
                return f"{date_str}-01-01"
        except Exception as e:
            logger.warning(f"Data inválida: {date_str} - {str(e)}")
            return None

    def import_csv_data():
        stats = {
            'reservatorios': set(),
            'municipios': set(),
            'registros_salvos': 0
        }
        
        try:
            csv.field_size_limit(sys.maxsize)
            
            with open(CSV_PATH, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                
                records = []
                for row in reader:
                    nome = sanitize_str(row.get('nome'))
                    municipio = sanitize_str(row.get('nome_municipio'))
                    
                    if nome:
                        stats['reservatorios'].add(nome)
                    if municipio:
                        stats['municipios'].add(municipio)
                    
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
                        'geometry': row.get('geometry')
                    }
                    
                    records.append(record)
            
            if records:
                with engine.begin() as conn:
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
                    stats['registros_salvos'] = len(records)
                    logger.info(f"{len(records)} registros inseridos com sucesso")
            
            stats['qtd_reservatorios'] = len(stats['reservatorios'])
            stats['qtd_municipios'] = len(stats['municipios'])
            
            return stats
            
        except FileNotFoundError:
            logger.error(f"Arquivo CSV não encontrado: {CSV_PATH}")
            return None
        except Exception as e:
            logger.error(f"Erro durante importação: {str(e)}")
            return None

    try:
        create_table()
        stats = import_csv_data()
        return {"status": "success", "stats": stats} if stats else {"status": "error", "message": "Falha na importação"}
    except Exception as e:
        logger.error(f"Erro no processamento de reservatórios: {e}")
        return {"status": "error", "message": str(e)}

# Função principal unificada
def main():
    """Função principal que orquestra todas as importações"""
    logger.info("Iniciando importador unificado de dados geoespaciais")
    
    # Verificar conexão com o banco
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexão com o PostgreSQL OK")
    except Exception as e:
        logger.error(f"Falha na conexão com PostgreSQL: {str(e)}")
        return
    
    # Executar todos os importadores
    importers = [
        ("Assentamentos", import_assentamentos),
        ("Malha Fundiária", import_malha_fundiaria),
        ("Municípios", import_municipios),
        ("Regiões Administrativas", import_regioes_adm),
        ("Reservatórios", import_reservatorios)
    ]
    
    results = {}
    
    for name, importer_func in importers:
        logger.info(f"Processando: {name}")
        try:
            results[name] = importer_func()
            logger.info(f"Concluído: {name} - Status: {results[name].get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"Erro inesperado ao processar {name}: {str(e)}")
            results[name] = {"status": "error", "message": str(e)}
    
    # Exibir resumo
    print("\n=== RESUMO DA IMPORTAÇÃO UNIFICADA ===")
    for name, result in results.items():
        status = result.get('status', 'unknown')
        if status == 'success':
            stats = result.get('stats', {})
            print(f"{name}: ✓ SUCCESS (Registros: {stats.get('registros_salvos', 'N/A')})")
        elif status == 'skipped':
            print(f"{name}: ⤷ SKIPPED ({result.get('reason', 'Dados recentes')})")
        else:
            print(f"{name}: ✗ ERROR ({result.get('message', 'Erro desconhecido')})")
    print("======================================")

if __name__ == "__main__":
    main()