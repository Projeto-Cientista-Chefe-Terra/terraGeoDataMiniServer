# data_service/main.py

import os
import json
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

app = FastAPI(title="terraGeoDataMiniServer")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# 1) Compressão GZip e Brotli
app.add_middleware(GZipMiddleware, minimum_size=500, compresslevel=5)
app.add_middleware(BrotliMiddleware, quality=5)

# 2) Helpers de engine e SQL simplificado
@lru_cache()
def get_engine():
    return get_sqlalchemy_engine()

def _ci_equals(column: str, param: str = "param") -> str:
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return f"LOWER({column}) = LOWER(:{param})"
    return f"{column} ILIKE :{param}"

def _geom_sql(tolerance: float = 0.0001, decimals: int = 6) -> str:
    """
    ST_Simplify + ST_AsGeoJSON para geometria mais leve.
    """
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return f"AsGeoJSON(ST_Simplify(geometry, {tolerance}), {decimals})"
    return f"ST_AsGeoJSON(ST_Simplify(wkb_geometry, {tolerance}), {decimals})"

# 3) Cache das listas de regiões e municípios
@lru_cache(maxsize=1)
def fetch_regioes() -> list[str]:
    sql = f"""
        SELECT DISTINCT regiao_administrativa
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE regiao_administrativa IS NOT NULL
        ORDER BY regiao_administrativa
    """
    with get_engine().connect() as conn:
        # .mappings() garante RowMapping (dict[str,Any]) em vez de Row :contentReference[oaicite:2]{index=2}
        rows = conn.execute(text(sql)).mappings().all()
    return [r['regiao_administrativa'] for r in rows]

@lru_cache(maxsize=128)
def fetch_municipios(regiao: str) -> list[str]:
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

@app.get("/regioes")
def listar_regioes():
    return {"regioes": fetch_regioes()}

@app.get("/municipios")
def listar_municipios(regiao: str = Query(..., description="Região (case-insensitive)")):
    municipios = fetch_municipios(regiao)
    if not municipios:
        raise HTTPException(404, f"Região '{regiao}' não encontrada ou sem municípios.")
    return {"municipios": municipios}

# 4) Pré-processamento paralelo para municípios
def _preprocess_municipio(muni: str):
    sql = (
        f"SELECT {_geom_sql()} AS geom_json, \"nm_mun\" AS nome_municipio "
        f"FROM {settings.TABLE_GEOM_MUNICIPIOS} "
        f"WHERE {_ci_equals('\"nm_mun\"', 'muni')}"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"muni": muni}).mappings().all()
    # row_to_feature espera um dict e cuida do json.loads 
    features = [row_to_feature(row) for row in rows if row.get('geom_json')]
    geojson = {"type": "FeatureCollection", "features": features}
    os.makedirs("data/geodata", exist_ok=True)
    path = f"data/geodata/municipio_{muni}.geojson"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f)

# 5) Pré-processamento paralelo para regiões
def _preprocess_regiao(reg: str):
    prop_cols = [
        "numero_lote", "numero_incra", "situacao_juridica",
        "modulo_fiscal", "area", "nome_municipio",
        "regiao_administrativa", "categoria", "nome_municipio_original"
    ]
    select_props = ", ".join(f'"{c}"' for c in prop_cols)
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
    muni_list = []
    for reg in fetch_regioes():
        muni_list.extend(fetch_municipios(reg))
    with Pool(processes=cpu_count()) as pool:
        pool.map(_preprocess_municipio, set(muni_list))
    with Pool(processes=cpu_count()) as pool:
        pool.map(_preprocess_regiao, fetch_regioes())

# 6) Scheduler diário às 02:00
scheduler = BackgroundScheduler()
scheduler.add_job(preprocess_geojson, 'cron', hour=2, minute=0)
scheduler.start()

# 7) Endpoints com fallback para arquivos pré-processados
@app.get("/geojson_muni")
def obter_geojson_municipio(municipio: str = Query(..., description="Município (case-insensitive)")):
    file_path = f"data/geodata/municipio_{municipio}.geojson"
    if os.path.isfile(file_path):
        return json.load(open(file_path, encoding="utf-8"))
    # fallback on-the-fly
    sql = (
        f"SELECT {_geom_sql()} AS geom_json, \"nm_mun\" AS nome_municipio "
        f"FROM {settings.TABLE_GEOM_MUNICIPIOS} "
        f"WHERE {_ci_equals('\"nm_mun\"', 'municipio')}"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"municipio": municipio}).mappings().all()
    features = [row_to_feature(row) for row in rows if row.get('geom_json')]
    if not features:
        raise HTTPException(404, f"Município '{municipio}' não encontrado.")
    return {"type": "FeatureCollection", "features": features}

@app.get("/geojson")
def obter_geojson(
    regiao: str = Query(None, description="Região (case-insensitive)"),
    municipio: str = Query(None, description="Município (case-insensitive)")
):
    if bool(regiao) == bool(municipio):
        raise HTTPException(400, "Informe exatamente 'regiao' OU 'municipio'.")
    filtro, valor = ("regiao", regiao) if regiao else ("municipio", municipio)
    file_path = f"data/geodata/{filtro}_{valor}.geojson"
    if os.path.isfile(file_path):
        return json.load(open(file_path, encoding="utf-8"))
    # on-the-fly
    column = "regiao_administrativa" if regiao else "nome_municipio"
    sql = f"""
        SELECT {_geom_sql()} AS geom_json, {', '.join(f'"{c}"' for c in [
            "numero_lote","numero_incra","situacao_juridica",
            "modulo_fiscal","area","nome_municipio",
            "regiao_administrativa","categoria","nome_municipio_original"
        ])}
        FROM {settings.TABLE_DADOS_FUNDIARIOS}
        WHERE {_ci_equals(column, 'param')};
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"param": valor}).mappings().all()
    features = [row_to_feature(row) for row in rows if row.get('geom_json')]
    if not features:
        raise HTTPException(404, f"Nenhuma geometria para filtro '{valor}'.")
    return {"type": "FeatureCollection", "features": features}
