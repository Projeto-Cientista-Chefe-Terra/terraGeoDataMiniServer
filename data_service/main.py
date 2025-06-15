from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from config import settings, DatabaseType
from .db import get_sqlalchemy_engine
from .utils import row_to_feature

app = FastAPI(title="terraGeoDataMiniServer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

TABLE_GEOM_MUNICIPIOS = "municipios_ceara"
TABLE_DADOS_FUNDIARIOS = "malha_fundiaria_ceara"

def _ci_equals(column: str, param_name: str = "param") -> str:
    """
    Gera cláusula case-insensitive:
    - SQLite:   LOWER(col) = LOWER(:param)
    - Postgres: col ILIKE :param
    """
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return f"LOWER({column}) = LOWER(:{param_name})"
    else:
        return f"{column} ILIKE :{param_name}"


def _geom_sql() -> str:
    """
    Retorna a expressão SQL para extrair GeoJSON:
    - SQLite+SpatiaLite: AsGeoJSON(geometry)
    - Postgres/PostGIS:  ST_AsGeoJSON(wkb_geometry)
    """
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        return "AsGeoJSON(geometry)"
    return "ST_AsGeoJSON(wkb_geometry)"

@app.get("/regioes")
def listar_regioes():
    engine = get_sqlalchemy_engine()
    sql = f"""
        SELECT DISTINCT regiao_administrativa
        FROM {TABLE_DADOS_FUNDIARIOS}
        WHERE regiao_administrativa IS NOT NULL
        ORDER BY regiao_administrativa;
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {"regioes": [r[0] for r in rows]}

@app.get("/municipios")
def listar_municipios(
    regiao: str = Query(..., description="Região administrativa (case-insensitive)")
):
    engine = get_sqlalchemy_engine()
    where = _ci_equals("regiao_administrativa", "regiao")
    sql = f"""
        SELECT DISTINCT nome_municipio
        FROM {TABLE_DADOS_FUNDIARIOS}
        WHERE {where}
          AND nome_municipio IS NOT NULL
        ORDER BY nome_municipio;
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"regiao": regiao}).fetchall()
    if not rows:
        raise HTTPException(404, f"Região '{regiao}' não encontrada ou sem municípios.")
    return {"municipios": [r[0] for r in rows]}

@app.get("/geojson_muni")
def obter_geojson_municipio(
    municipio: str = Query(..., description="Município (case-insensitive)")
):
    engine = get_sqlalchemy_engine()
    geom_expr = _geom_sql()
    where = _ci_equals('"nm_mun"', "municipio")

    sql = f"""
        SELECT {geom_expr} AS geom_json,
               "nm_mun" AS nome_municipio
        FROM {TABLE_GEOM_MUNICIPIOS}
        WHERE {where};
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"municipio": municipio}).fetchall()

    features = [
        row_to_feature(r, ["geom_json", "nome_municipio"] )
        for r in rows if r[0]
    ]
    if not features:
        raise HTTPException(404, f"Município '{municipio}' não encontrado.")
    return {"type": "FeatureCollection", "features": features}

@app.get("/geojson")
def obter_geojson(
    regiao: str = Query(None, description="Região administrativa (case-insensitive)"),
    municipio: str = Query(None, description="Município (case-insensitive)")
):
    # deve vir exatamente um filtro
    if bool(regiao) == bool(municipio):
        raise HTTPException(
            400,
            "Informe exatamente um dos parâmetros: 'regiao' OU 'municipio'."
        )

    engine = get_sqlalchemy_engine()
    geom_expr = _geom_sql()

    # monta filtro dinâmico
    if regiao:
        where, param = _ci_equals("regiao_administrativa", "param"), regiao
    else:
        where, param = _ci_equals("nome_municipio", "param"), municipio

    prop_cols = [
        "numero_lote", "numero_incra", "situacao_juridica",
        "modulo_fiscal", "area", "nome_municipio",
        "regiao_administrativa", "categoria", "nome_municipio_original"
    ]
    props_select = ", ".join(f'"{c}"' for c in prop_cols)

    sql = f"""
        SELECT {geom_expr} AS geom_json,
               {props_select}
        FROM {TABLE_DADOS_FUNDIARIOS}
        WHERE {where};
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"param": param}).fetchall()

    features = [
        row_to_feature(r, ["geom_json"] + prop_cols)
        for r in rows if r[0]
    ]
    if not features:
        filtro = f"região '{regiao}'" if regiao else f"município '{municipio}'"
        raise HTTPException(404, f"Nenhuma geometria encontrada para o filtro {filtro}.")
    return {"type": "FeatureCollection", "features": features}
