# data_service/main.py

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from .db import get_sqlalchemy_engine, get_sqlite_connection
from .utils import row_to_feature

app = FastAPI(title="terraGeoDataMiniServer_SpatiaLite")

# Permite que qualquer front-end faça requisições CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

TABLE_MUNICIPIOS = "municipios_ceara"
TABLE_FUNDOS      = "malha_fundiaria_ceara"

# ---------------------------------------------------------
# 1) GET /regioes
# ---------------------------------------------------------
@app.get("/regioes")
def listar_regioes():
    engine = get_sqlalchemy_engine()
    sql = f"""
        SELECT DISTINCT regiao_administrativa
          FROM "{TABLE_FUNDOS}"
         WHERE regiao_administrativa IS NOT NULL
         ORDER BY regiao_administrativa;
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            regioes = [row[0] for row in result.fetchall()]
        return {"regioes": regioes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar regiões: {e}")


# ---------------------------------------------------------
# 2) GET /municipios?regiao=XYZ
#    Lista municípios de uma dada região (case-insensitive)
# ---------------------------------------------------------
@app.get("/municipios")
def listar_municipios(
    regiao: str = Query(..., description="Região administrativa (case-insensitive)")
):
    sql = f"""
        SELECT DISTINCT nome_municipio
          FROM "{TABLE_FUNDOS}"
         WHERE regiao_administrativa = ? COLLATE NOCASE
           AND nome_municipio IS NOT NULL
         ORDER BY nome_municipio;
    """
    try:
        with get_sqlite_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT load_extension('mod_spatialite');")
            cur.execute(sql, (regiao,))
            rows = cur.fetchall()
            municipios = [r[0] for r in rows]
        if not municipios:
            raise HTTPException(
                status_code=404,
                detail=f"Região '{regiao}' não encontrada ou sem municípios."
            )
        return {"municipios": municipios}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar municípios: {e}")


# ---------------------------------------------------------
# 3) GET /geojson?regiao=XYZ  ou  ?municipio=YYY
#    Retorna um GeoJSON da malha fundiária,
#    filtrando case-insensitive.
#    Agora, em "municipio", filtramos pelo campo correto da tabela
#    malha_fundiaria_ceara, que é "nome_municipio" (caso este exista lá).
# ---------------------------------------------------------
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
        where_clause = "regiao_administrativa = ? COLLATE NOCASE"
        param = regiao
    else:
        # Aqui filtramos pelo nome do município na malha fundiária
        where_clause = "nome_municipio = ? COLLATE NOCASE"
        param = municipio

    prop_cols = [
        "modulo_fiscal",
        "area",
        "nome_municipio",
        "regiao_administrativa",
        "categoria",
        "municipio_norm"
    ]
    props_select = ", ".join(prop_cols)

    sql = f"""
        SELECT AsGeoJSON(geometry) AS geom_json, {props_select}
          FROM "{TABLE_FUNDOS}"
         WHERE {where_clause};
    """
    try:
        features = []
        with get_sqlite_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT load_extension('mod_spatialite');")
            cur.execute(sql, (param,))
            rows = cur.fetchall()
            colnames = ["geom_json"] + prop_cols

            if not rows:
                filtro = f"região '{regiao}'" if regiao else f"município '{municipio}'"
                raise HTTPException(
                    status_code=404,
                    detail=f"Nenhuma geometria encontrada para o filtro {filtro}."
                )

            for row in rows:
                features.append(row_to_feature(row, colnames))

        return {"type": "FeatureCollection", "features": features}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter GeoJSON: {e}")


# ---------------------------------------------------------
# 4) GET /geojson_muni?municipio=YYY
#    Retorna um GeoJSON do polígono (multipolígonos) do município,
#    filtrando pelo campo correto “NM_MUN” na tabela municipios_ceara.
# ---------------------------------------------------------
@app.get("/geojson_muni")
def obter_geojson_municipio(
    municipio: str = Query(..., description="Município (case-insensitive)")
):
    # Usamos NM_MUN (nome original do campo do GeoJSON) na tabela municipios_ceara
    sql = f"""
        SELECT AsGeoJSON(geometry) AS geom_json, NM_MUN AS nome_municipio
          FROM "{TABLE_MUNICIPIOS}"
         WHERE NM_MUN = ? COLLATE NOCASE;
    """
    try:
        features = []
        with get_sqlite_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT load_extension('mod_spatialite');")
            cur.execute(sql, (municipio,))
            rows = cur.fetchall()
            # Colnames: primeiro vem geom_json, depois “nome_municipio” (nome que demos a NM_MUN)
            colnames = ["geom_json", "nome_municipio"]

            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"Município '{municipio}' não encontrado."
                )

            for row in rows:
                features.append(row_to_feature(row, colnames))

        return {"type": "FeatureCollection", "features": features}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter GeoJSON de município: {e}")
