# data_service/main.py

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from .db import get_sqlalchemy_engine
from .utils import row_to_feature

app = FastAPI(title="terraGeoDataMiniServer_PostGIS")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

TABLE_MUNICIPIOS = "municipios_ceara"
TABLE_FUNDOS = "malha_fundiaria_ceara"

@app.get("/regioes")
def listar_regioes():
    engine = get_sqlalchemy_engine()
    sql = f"""
        SELECT DISTINCT regiao_administrativa
        FROM {TABLE_FUNDOS}
        WHERE regiao_administrativa IS NOT NULL
        ORDER BY regiao_administrativa;
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        regioes = [row[0] for row in result.fetchall()]
    return {"regioes": regioes}

@app.get("/municipios")
def listar_municipios(
    regiao: str = Query(..., description="Região administrativa (case-insensitive)")
):
    engine = get_sqlalchemy_engine()
    sql = f"""
        SELECT DISTINCT nome_municipio
        FROM {TABLE_FUNDOS}
        WHERE regiao_administrativa ILIKE :regiao
        AND nome_municipio IS NOT NULL
        ORDER BY nome_municipio;
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"regiao": regiao})
        municipios = [row[0] for row in result.fetchall()]
    if not municipios:
        raise HTTPException(
            status_code=404,
            detail=f"Região '{regiao}' não encontrada ou sem municípios."
        )
    return {"municipios": municipios}

@app.get("/geojson_muni")
def obter_geojson_municipio(
    municipio: str = Query(..., description="Município (case-insensitive)")
):
    sql = f"""
        SELECT ST_AsGeoJSON(wkb_geometry) AS geom_json, "nm_mun" AS nome_municipio
        FROM {TABLE_MUNICIPIOS}
        WHERE "nm_mun" ILIKE :municipio;
    """
    with get_sqlalchemy_engine().connect() as conn:
        result = conn.execute(text(sql), {"municipio": municipio})
        rows = result.fetchall()
        colnames = ["geom_json", "nome_municipio"]
        features = [row_to_feature(row, colnames) for row in rows if row[0]]
        if not features:
            raise HTTPException(
                status_code=404,
                detail=f"Município '{municipio}' não encontrado."
            )
    return {"type": "FeatureCollection", "features": features}




@app.get("/geojson")
def obter_geojson(
    regiao: str = Query(None, description="Região administrativa (case-insensitive)"),
    municipio: str = Query(None, description="Município (case-insensitive)")
):
    if (regiao and municipio) or (not regiao and not municipio):
        raise HTTPException(
            status_code=400,
            detail="Informe exatamente um dos parâmetros: 'regiao' OU 'municipio'."
        )
    if regiao:
        where_clause = "regiao_administrativa ILIKE :param"
        param = regiao
    else:
        where_clause = "nome_municipio ILIKE :param"
        param = municipio

    prop_cols = [
        "numero_lote",
        "numero_incra",
        "situacao_juridica",        
        "modulo_fiscal",
        "area",
        "nome_municipio",
        "regiao_administrativa",
        "categoria",
        "nome_municipio_original"
    ]
    props_select = ", ".join([f'"{col}"' for col in prop_cols])
    sql = f"""
        SELECT ST_AsGeoJSON(wkb_geometry) AS geom_json, {props_select}
        FROM {TABLE_FUNDOS}
        WHERE {where_clause};
    """
    with get_sqlalchemy_engine().connect() as conn:
        result = conn.execute(text(sql), {"param": param})
        rows = result.fetchall()
        colnames = ["geom_json"] + prop_cols
        features = [row_to_feature(row, colnames) for row in rows if row[0]]
        if not features:
            filtro = f"região '{regiao}'" if regiao else f"município '{municipio}'"
            raise HTTPException(
                status_code=404,
                detail=f"Nenhuma geometria encontrada para o filtro {filtro}."
            )
    return {"type": "FeatureCollection", "features": features}