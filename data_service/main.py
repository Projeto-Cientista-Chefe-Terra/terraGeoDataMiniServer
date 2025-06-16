# data_service/main.py

import os
import json
import logging
from typing import List, Dict, Any, Optional
from multiprocessing import Pool, cpu_count
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from brotli_asgi import BrotliMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from pythonjsonlogger import jsonlogger
from sqlalchemy import text

from config import settings, DatabaseType
from .db import get_sqlalchemy_engine
from .utils import row_to_feature
from functools import lru_cache

# ==================== Configuração de Logs ====================
logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(message)s %(module)s %(funcName)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# ==================== Ciclo de Vida da Aplicação ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando aplicação...")
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexão com o banco de dados estabelecida")
    except Exception as e:
        logger.error(f"❌ Falha ao conectar ao banco de dados: {e}")
        raise

    scheduler = BackgroundScheduler()
    scheduler.add_job(preprocess_geojson, 'cron',
                      hour=settings.PREPROCESS_START_HOUR,
                      minute=settings.PREPROCESS_START_MINUTE)
    scheduler.start()

    yield

    logger.info("Encerrando aplicação...")
    scheduler.shutdown()
    get_engine().dispose()

# ==================== App FastAPI ====================
app = FastAPI(
    title="terraGeoDataMiniServer",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=500, compresslevel=5)
app.add_middleware(BrotliMiddleware, quality=5)

# ==================== Constantes ====================
COMMON_PROPERTY_COLUMNS = [
    "numero_lote", "numero_incra", "situacao_juridica",
    "modulo_fiscal", "area", "nome_municipio",
    "regiao_administrativa", "categoria", "nome_municipio_original"
]

# ==================== Helpers de Engine e SQL ====================
@lru_cache()
def get_engine():
    return get_sqlalchemy_engine()

def _ci_equals(column: str, param: str = "param") -> str:
    """Cláusula case-insensitive para SQLite ou Postgres."""
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return f"LOWER({column}) = LOWER(:{param})"
    return f"{column} ILIKE :{param}"

def _geom_sql(
    tolerance: Optional[float] = None,
    decimals: Optional[int] = None
) -> str:
    """
    Expressão SQL para GeoJSON simplificado.
    Usa settings.GEOMETRY_TOLERANCE e settings.GEOMETRY_DECIMALS por padrão.
    """
    tol = tolerance if tolerance is not None else settings.GEOMETRY_TOLERANCE
    dec = decimals if decimals is not None else settings.GEOMETRY_DECIMALS
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return f"AsGeoJSON(ST_Simplify(geometry, {tol}), {dec})"
    return f"ST_AsGeoJSON(ST_Simplify(wkb_geometry, {tol}), {dec})"

# ==================== Listagem de Regiões e Municípios ====================
@lru_cache(maxsize=1)
def fetch_regioes() -> List[str]:
    """Retorna todas as regiões administrativas."""
    sql = f"""
        SELECT DISTINCT regiao_administrativa
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE regiao_administrativa IS NOT NULL
        ORDER BY regiao_administrativa
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return [r['regiao_administrativa'] for r in rows]

@lru_cache(maxsize=32)
def fetch_municipios(regiao: str) -> List[str]:
    """Retorna municípios de uma região."""
    where = _ci_equals("regiao_administrativa", "regiao")
    sql = f"""
        SELECT DISTINCT nome_municipio
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE {where} AND nome_municipio IS NOT NULL
        ORDER BY nome_municipio
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"regiao": regiao}).mappings().all()
    return [r['nome_municipio'] for r in rows]

# ==================== GeoJSON Genérico ====================
def _get_geojson_from_file_or_db(
    entity_type: str,
    entity_name: str,
    table: str,
    where_column: str,
    extra_columns: Optional[List[str]] = None,
    tolerance: Optional[float] = None,
    decimals: Optional[int] = None,
) -> Dict[str, Any]:
    """Tenta ler arquivo pré-processado ou consulta o banco."""
    file_path = f"data/geodata/{entity_type}_{entity_name}.geojson"
    if os.path.isfile(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    cols = extra_columns or []
    props = ", ".join(f'"{c}"' for c in cols)
    geom = _geom_sql(tolerance=tolerance, decimals=decimals)
    sql = f"""
        SELECT {geom} AS geom_json, {props}
        FROM {table}
        WHERE {_ci_equals(where_column, 'param')}
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"param": entity_name}).mappings().all()
    features = [row_to_feature(r) for r in rows if r.get('geom_json')]
    if not features:
        raise HTTPException(404, f"Nenhuma geometria para {entity_type} '{entity_name}'")
    return {"type": "FeatureCollection", "features": features}

# ==================== Pré-processamento ====================
def _preprocess_municipio(muni: str):
    """Gera e salva GeoJSON de município."""
    sql = (
        f"SELECT {_geom_sql()} AS geom_json, \"nm_mun\" AS nome_municipio "
        f"FROM {settings.TABLE_GEOM_MUNICIPIOS} "
        f"WHERE {_ci_equals('nm_mun', 'muni')}"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"muni": muni}).mappings().all()
    features = [row_to_feature(r) for r in rows if r.get('geom_json')]
    os.makedirs("data/geodata", exist_ok=True)
    with open(f"data/geodata/municipio_{muni}.geojson", "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)

def _preprocess_regiao(reg: str):
    """Gera e salva GeoJSON de região."""
    cols = ", ".join(f'"{c}"' for c in COMMON_PROPERTY_COLUMNS)
    sql = (
        f"SELECT {_geom_sql()} AS geom_json, {cols} "
        f"FROM {settings.TABLE_DADOS_FUNDIARIOS} "
        f"WHERE {_ci_equals('regiao_administrativa', 'param')}"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"param": reg}).mappings().all()
    features = [row_to_feature(r) for r in rows if r.get('geom_json')]
    os.makedirs("data/geodata", exist_ok=True)
    with open(f"data/geodata/regiao_{reg}.geojson", "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)

def preprocess_geojson():
    """Dispara pré-processamento paralelo."""
    muni_list = []
    for reg in fetch_regioes():
        muni_list.extend(fetch_municipios(reg))
    with Pool(cpu_count()) as pool:
        pool.map(_preprocess_municipio, set(muni_list))
    with Pool(cpu_count()) as pool:
        pool.map(_preprocess_regiao, fetch_regioes())

# ==================== Endpoints ====================
@app.get("/health")
def health_check():
    """Verifica saúde do serviço."""
    with get_engine().connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "healthy"}

@app.get("/regioes")
def listar_regioes():
    """Lista todas as regiões."""
    return {"regioes": fetch_regioes()}

@app.get("/municipios")
def listar_municipios(regiao: str = Query(..., description="Região case-insensitive.")):
    """Lista municípios de uma região."""
    munis = fetch_municipios(regiao)
    if not munis:
        raise HTTPException(404, f"Região '{regiao}' não encontrada.")
    return {"municipios": munis}

@app.get("/municipios_todos")
def listar_todos_municipios():
    """Lista todos municípios."""
    sql = f"""
        SELECT DISTINCT nome_municipio
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE nome_municipio IS NOT NULL
        ORDER BY nome_municipio
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {"municipios": [r[0] for r in rows]}

@app.get("/geojson_muni")
def geojson_muni(municipio: str = Query(..., description="Município case-insensitive.")):
    """GeoJSON de município."""
    geom_expr = _geom_sql()
    where = _ci_equals("\"nm_mun\"", "municipio")
    sql = f"""
        SELECT {geom_expr} AS geom_json,
               \"nm_mun\" AS nome_municipio
        FROM {settings.TABLE_GEOM_MUNICIPIOS}
        WHERE {where};
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"municipio": municipio}).mappings().all()
    features = [row_to_feature(r) for r in rows if r.get('geom_json')]
    if not features:
        raise HTTPException(404, f"Município '{municipio}' não encontrado.")
    return {"type": "FeatureCollection", "features": features}

@app.get("/geojson")
def geojson(
    regiao: str = Query(None),
    municipio: str = Query(None),
    tolerance: Optional[float] = Query(None, description="Tolerância de simplificação da geometria (opcional)"),
    decimals: Optional[int] = Query(None, description="Número de casas decimais na geometria (opcional)"),
    limit: int = Query(1000)
):
    """GeoJSON de região ou município."""
    if bool(regiao) == bool(municipio):
        raise HTTPException(400, "Informe 'regiao' OU 'municipio'.")
    if regiao:
        return _get_geojson_from_file_or_db(
            "regiao", regiao,
            settings.TABLE_DADOS_FUNDIARIOS,
            'regiao_administrativa',
            COMMON_PROPERTY_COLUMNS,
            tolerance=tolerance,
            decimals=decimals
        )
    return _get_geojson_from_file_or_db(
        "municipio", municipio,
        settings.TABLE_DADOS_FUNDIARIOS,
        'nome_municipio',
        COMMON_PROPERTY_COLUMNS,
        tolerance=tolerance,
        decimals=decimals
    )

@app.get("/dados_fundiarios")
def dados_fundiarios(
    regiao: str = Query(None),
    municipio: str = Query(None)
):
    """Dados tabulares (sem geometria)."""
    if bool(regiao) == bool(municipio):
        raise HTTPException(400, "Informe 'regiao' OU 'municipio'.")
    where, val = (
        ('regiao_administrativa', regiao) if regiao else ('nome_municipio', municipio)
    )
    props = ", ".join(f'"{c}"' for c in COMMON_PROPERTY_COLUMNS[:-1])
    sql = f"""
        SELECT {props}
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE {_ci_equals(where, 'param')}
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"param": val}).fetchall()
    if not rows:
        raise HTTPException(404, "Nenhum dado encontrado.")
    return [dict(zip(COMMON_PROPERTY_COLUMNS[:-1], r)) for r in rows]
