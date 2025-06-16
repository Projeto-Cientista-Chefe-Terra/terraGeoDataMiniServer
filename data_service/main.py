# data_service/main.py

import os
import json
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from brotli_asgi import BrotliMiddleware
from sqlalchemy import text
from config import settings, DatabaseType
from .db import get_sqlalchemy_engine
from .utils import row_to_feature
from functools import lru_cache
from multiprocessing import Pool, cpu_count
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from pythonjsonlogger import jsonlogger
from contextlib import asynccontextmanager
from fastapi import FastAPI
from contextlib import asynccontextmanager


########################################## Configuracaoes ##########################################

# Cofiguracao do logging
logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(message)s %(module)s %(funcName)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)



########################################## Inicializacao da API ##########################################

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    logger.info("Iniciando aplicação...")
    
    try:
        # Verificação de conexão com o banco
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexão com o banco de dados estabelecida")
    except Exception as e:
        logger.error(f"❌ Falha ao conectar ao banco de dados: {str(e)}")
        raise

    # Inicia o scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(preprocess_geojson, 'cron', hour=2, minute=0)
    scheduler.start()
    
    yield
    
    # Shutdown logic
    logger.info("Encerrando aplicação...")
    scheduler.shutdown()
    get_engine().dispose()

# Criação ÚNICA do app FastAPI
app = FastAPI(
    title="terraGeoDataMiniServer",
    lifespan=lifespan
)

# Configurações de CORS mais seguras
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],  # Somente leitura
    allow_headers=["*"],
)

# Compressão
app.add_middleware(GZipMiddleware, minimum_size=500, compresslevel=5)
app.add_middleware(BrotliMiddleware, quality=5)



########################################## Constantes para evitar repetição ##########################################

COMMON_PROPERTY_COLUMNS = [
    "numero_lote", "numero_incra", "situacao_juridica",
    "modulo_fiscal", "area", "nome_municipio",
    "regiao_administrativa", "categoria", "nome_municipio_original"
]

########################################## Helpers de engine e SQL ##########################################
@lru_cache()
def get_engine():
    return get_sqlalchemy_engine()

def _ci_equals(column: str, param: str = "param") -> str:
    """Gera condição SQL case-insensitive de acordo com o banco"""
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return f"LOWER({column}) = LOWER(:{param})"
    return f"{column} ILIKE :{param}"

def _geom_sql(tolerance: float = 0.0001, decimals: int = 6) -> str:
    """Gera SQL para geometria simplificada"""
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return f"AsGeoJSON(ST_Simplify(geometry, {tolerance}), {decimals})"
    return f"ST_AsGeoJSON(ST_Simplify(wkb_geometry, {tolerance}), {decimals})"


########################################## Cache das listas de regiões e municípios ##########################################
@lru_cache(maxsize=1)
def fetch_regioes() -> List[str]:
    """Obtém todas as regiões administrativas"""
    sql = f"""
        SELECT DISTINCT regiao_administrativa
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE regiao_administrativa IS NOT NULL
        ORDER BY regiao_administrativa
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return [r['regiao_administrativa'] for r in rows]

@lru_cache(maxsize=32)  # Cache para ~32 regiões
def fetch_municipios(regiao: str) -> List[str]:
    """Obtém municípios de uma região com cache"""
    where = _ci_equals("regiao_administrativa", "regiao")
    sql = f"""
        SELECT DISTINCT nome_municipio
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE {where}
          AND nome_municipio IS NOT NULL
        ORDER BY nome_municipio
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"regiao": regiao}).mappings().all()
    return [r['nome_municipio'] for r in rows]


########################################## Função auxiliar reutilizável ##########################################

def _get_geojson_from_file_or_db(
    entity_type: str,
    entity_name: str,
    table: str,
    where_column: str,
    extra_columns: List[str] = None
) -> Dict[str, Any]:
    """Obtém GeoJSON de arquivo pré-processado ou gera on-the-fly"""
    file_path = f"data/geodata/{entity_type}_{entity_name}.geojson"
    if os.path.isfile(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    columns = extra_columns if extra_columns else []
    select_cols = ", ".join(f'"{c}"' for c in columns)
    geom_select = _geom_sql()
    
    sql = f"""
        SELECT {geom_select} AS geom_json, {select_cols}
        FROM {table}
        WHERE {_ci_equals(where_column, 'param')}
    """
    
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"param": entity_name}).mappings().all()
    
    features = [row_to_feature(row) for row in rows if row.get('geom_json')]
    if not features:
        raise HTTPException(404, f"Nenhuma geometria para {entity_type} '{entity_name}'.")
    
    return {"type": "FeatureCollection", "features": features}


########################################## Pré-processamento paralelo ##########################################

def _preprocess_municipio(muni: str):
    """Pré-processa dados de um município"""
    sql = (
        f"SELECT {_geom_sql()} AS geom_json, \"nm_mun\" AS nome_municipio "
        f"FROM {settings.TABLE_GEOM_MUNICIPIOS} "
        f"WHERE {_ci_equals('\"nm_mun\"', 'muni')}"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"muni": muni}).mappings().all()
    
    features = [row_to_feature(row) for row in rows if row.get('geom_json')]
    geojson = {"type": "FeatureCollection", "features": features}
    os.makedirs("data/geodata", exist_ok=True)
    path = f"data/geodata/municipio_{muni}.geojson"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f)

def _preprocess_regiao(reg: str):
    """Pré-processa dados de uma região"""
    select_props = ", ".join(f'"{c}"' for c in COMMON_PROPERTY_COLUMNS)
    sql = (
        f"SELECT {_geom_sql()} AS geom_json, {select_props} "
        f"FROM {settings.TABLE_DADOS_FUNDIARIOS} "
        f"WHERE {_ci_equals('regiao_administrativa', 'param')}"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"param": reg}).mappings().all()
    
    features = [row_to_feature(row) for row in rows if row.get('geom_json')]
    geojson = {"type": "FeatureCollection", "features": features}
    os.makedirs("data/geodata", exist_ok=True)
    path = f"data/geodata/regiao_{reg}.geojson"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f)

def preprocess_geojson():
    """Executa pré-processamento em paralelo"""
    muni_list = []
    for reg in fetch_regioes():
        muni_list.extend(fetch_municipios(reg))
    
    with Pool(processes=cpu_count()) as pool:
        pool.map(_preprocess_municipio, set(muni_list))
    with Pool(processes=cpu_count()) as pool:
        pool.map(_preprocess_regiao, fetch_regioes())





############## Endpoints #################

@app.get("/health")
def health_check():
    """Endpoint de verificação de saúde do serviço"""
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(500, detail="Service unavailable")


@app.get("/regioes")
def listar_regioes():
    """Lista todas as regiões administrativas disponíveis"""
    return {"regioes": fetch_regioes()}

@app.get("/municipios")
def listar_municipios(regiao: str = Query(..., description="Região (case-insensitive)")):
    """Lista municípios de uma região específica"""
    municipios = fetch_municipios(regiao)
    if not municipios:
        raise HTTPException(404, f"Região '{regiao}' não encontrada ou sem municípios.")
    return {"municipios": municipios}

@app.get("/municipios_todos")
def listar_todos_municipios():
    """Lista todos os municípios disponíveis"""
    sql = f"""
        SELECT DISTINCT nome_municipio
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE nome_municipio IS NOT NULL
        ORDER BY nome_municipio;
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {"municipios": [r[0] for r in rows]}

@app.get("/geojson_muni")
def obter_geojson_municipio(municipio: str = Query(..., description="Município (case-insensitive)")):
    """Obtém GeoJSON para um município específico"""
    return _get_geojson_from_file_or_db(
        "municipio",
        municipio,
        settings.TABLE_GEOM_MUNICIPIOS,
        '"nm_mun"',
        ["nm_mun AS nome_municipio"]
    )

@app.get("/geojson")
def obter_geojson(
    regiao: str = Query(None, description="Região (case-insensitive)"),
    municipio: str = Query(None, description="Município (case-insensitive)")
):
    """Obtém GeoJSON para região ou município"""
    if bool(regiao) == bool(municipio):
        raise HTTPException(400, "Informe exatamente 'regiao' OU 'municipio'.")
    
    if regiao:
        return _get_geojson_from_file_or_db(
            "regiao",
            regiao,
            settings.TABLE_DADOS_FUNDIARIOS,
            "regiao_administrativa",
            COMMON_PROPERTY_COLUMNS
        )
    return _get_geojson_from_file_or_db(
        "municipio",
        municipio,
        settings.TABLE_DADOS_FUNDIARIOS,
        "nome_municipio",
        COMMON_PROPERTY_COLUMNS
    )

@app.get("/dados_fundiarios")
def obter_dados_fundiarios(
    regiao: str = Query(None, description="Região administrativa (case-insensitive)"),
    municipio: str = Query(None, description="Município (case-insensitive)")
):
    """Retorna dados tabulares (sem geometria) para análise"""
    if bool(regiao) == bool(municipio):
        raise HTTPException(400, "Informe exatamente 'regiao' OU 'municipio'.")

    where, param = (_ci_equals("regiao_administrativa", "param"), regiao) if regiao else \
                   (_ci_equals("nome_municipio", "param"), municipio)

    props_select = ", ".join(f'"{c}"' for c in COMMON_PROPERTY_COLUMNS[:-1])  # Exclui categoria

    sql = f"""
        SELECT {props_select}
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE {where};
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"param": param}).fetchall()

    if not rows:
        filtro = f"região '{regiao}'" if regiao else f"município '{municipio}'"
        raise HTTPException(404, f"Nenhum dado encontrado para {filtro}.")
    
    return [dict(zip(COMMON_PROPERTY_COLUMNS[:-1], row)) for row in rows]
