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

from typing import Optional
from geoalchemy2.functions import ST_AsGeoJSON, ST_Transform


from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from datetime import datetime, timedelta


# ==================== Configuração de Logs ====================
logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(message)s %(module)s %(funcName)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)




# =================== Esquema de Autenticação JWT ===================
security = HTTPBearer()

def create_jwt_token() -> str:
    expiration = datetime.utcnow() + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    payload = {
        "exp": expiration,
        "iat": datetime.utcnow(),
        "sub": "streamlit_app"
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(
            credentials.credentials, 
            settings.JWT_SECRET, 
            algorithms=[settings.JWT_ALGORITHM]
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado"
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido"
        )


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
    "modulo_fiscal", "area", "nome_municipio","nome_proprietario","nome_distrito","numero_titulo",
    "regiao_administrativa", "categoria", "nome_municipio_original","imovel","data_criacao_lote"
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
    return f"ST_AsGeoJSON(ST_Simplify(geometry, {tol}), {dec})"

# ==================== Listagem de Regiões e Municípios ====================
@lru_cache(maxsize=32)
def fetch_regioes(_: dict = Depends(verify_token)) -> List[str]:
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
def fetch_municipios(regiao: str,_: dict = Depends(verify_token)) -> List[str]:
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
    _: dict = Depends(verify_token),
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
def listar_regioes(_: dict = Depends(verify_token)):
    """Lista todas as regiões."""
    return {"regioes": fetch_regioes()}

@app.get("/municipios")
def listar_municipios(regiao: str = Query(..., description="Região case-insensitive."),_: dict = Depends(verify_token)):
    """Lista municípios de uma região."""
    munis = fetch_municipios(regiao)
    if not munis:
        raise HTTPException(404, f"Região '{regiao}' não encontrada.")
    return {"municipios": munis}

@app.get("/municipios_todos")
def listar_todos_municipios(_: dict = Depends(verify_token)):
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

# @app.get("/geojson_muni")
# def geojson_muni(municipio: str = Query(..., description="Município case-insensitive.")):
#     """GeoJSON de município."""
#     geom_expr = _geom_sql()
#     where = _ci_equals("\"nm_mun\"", "municipio")
#     sql = f"""
#         SELECT {geom_expr} AS geom_json,
#                \"nm_mun\" AS nome_municipio
#         FROM {settings.TABLE_GEOM_MUNICIPIOS}
#         WHERE {where};
#     """
#     with get_engine().connect() as conn:
#         rows = conn.execute(text(sql), {"municipio": municipio}).mappings().all()
#     features = [row_to_feature(r) for r in rows if r.get('geom_json')]
#     if not features:
#         raise HTTPException(404, f"Município '{municipio}' não encontrado.")
#     return {"type": "FeatureCollection", "features": features}

@app.get("/geojson_muni")
def geojson_muni(municipio: str = Query(..., description="Município case-insensitive ou 'todos' para retornar todos os municípios."),_: dict = Depends(verify_token)):
    """GeoJSON de município(s). Retorna todos se município='todos'."""
    geom_expr = _geom_sql()
    
    # Construção dinâmica da query SQL
    if municipio.lower() == "todos":
        sql = f"""
            SELECT {geom_expr} AS geom_json,
                   \"nm_mun\" AS nome_municipio
            FROM {settings.TABLE_GEOM_MUNICIPIOS};
        """
        params = {}
    else:
        where = _ci_equals("\"nm_mun\"", "municipio")
        sql = f"""
            SELECT {geom_expr} AS geom_json,
                   \"nm_mun\" AS nome_municipio
            FROM {settings.TABLE_GEOM_MUNICIPIOS}
            WHERE {where};
        """
        params = {"municipio": municipio}

    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    
    features = [row_to_feature(r) for r in rows if r.get('geom_json')]
    
    if not features:
        if municipio.lower() != "todos":
            raise HTTPException(404, f"Município '{municipio}' não encontrado.")
        else:
            raise HTTPException(404, "Nenhum município encontrado na base de dados.")
    
    return {
        "type": "FeatureCollection", 
        "features": features,
        "properties": {
            "total_municipios": len(features) if municipio.lower() == "todos" else 1
        }
    }

@app.get("/geojson")
def geojson(
    regiao: str = Query(None),
    municipio: str = Query(None),
    tolerance: Optional[float] = Query(None, description="Tolerância de simplificação da geometria (opcional)"),
    decimals: Optional[int] = Query(None, description="Número de casas decimais na geometria (opcional)"),
    _: dict = Depends(verify_token)
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
    municipio: str = Query(None),
    _: dict = Depends(verify_token)
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


@app.get("/geojson_assentamentos")
def geojson_assentamentos(
    municipio: str = Query("todos", description="Filtrar por município ('todos' para todos os municípios)"),
    tolerance: Optional[float] = Query(None, description="Tolerância de simplificação da geometria (opcional)"),
    decimals: Optional[int] = Query(None, description="Número de casas decimais na geometria (opcional)"),
    _: dict = Depends(verify_token)
):
    """
    Retorna todos os assentamentos estaduais do Ceará em formato GeoJSON.
    Pode ser filtrado por município ou retornar todos quando municipio=todos.
    """
    # Colunas que queremos retornar
    property_columns = [
        "cd_sipra", 
        "nome_municipio", 
        "nome_assentamento", 
        "nome_municipio_original", 
        "area", 
        "perimetro", 
        "tipo_assentamento", 
        "forma_obtecao", 
        "num_familias"
    ]


    cols = ", ".join(f'"{c}"' for c in property_columns)

    geom_expr = "wkt_geometry"

    if tolerance is not None:
        geom_expr = f"ST_SimplifyPreserveTopology({geom_expr}, {tolerance})"

    # Adicione 'options' para remover a dimensão Z
    if decimals is not None:
        geom_json_expr = f"ST_AsGeoJSON({geom_expr}, maxdecimaldigits := {decimals}, options := 1)"
    else:
        geom_json_expr = f"ST_AsGeoJSON({geom_expr}, options := 1)"

    sql = f"""
        SELECT {geom_json_expr} AS geom_json, {cols}
        FROM {settings.TABLE_DADOS_ASSENTAMENTOS}
    """

    params = {}

    if municipio and municipio.lower() != "todos":
        sql += f" WHERE {_ci_equals('nome_municipio', 'municipio')}"
        params["municipio"] = municipio

    try:        
        with get_engine().connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")




    features = []
    for row in rows:
        if not row.get('geom_json'):
            continue

        try:
            geom = json.loads(row['geom_json'])           
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "cd_sipra": row.get('cd_sipra'),
                    "nome_municipio": row.get('nome_municipio'),
                    "nome_assentamento": row.get('nome_assentamento'),
                    "nome_municipio_original": row.get('nome_municipio_original'),
                    "area": row.get('area'),
                    "perimetro": row.get('perimetro'),
                    "forma_obtecao": row.get('forma_obtecao'),
                    "tipo_assentamento": row.get('tipo_assentamento'),
                    "num_familias": row.get('num_familias')
                }
            })
        except Exception as e:
            print(f"Erro ao processar feature: {e}")
            continue

    if not features:
        raise HTTPException(status_code=404, detail=f"Nenhum assentamento encontrado{f' para {municipio}' if municipio != 'todos' else ''}")
    

    return {
        "type": "FeatureCollection",
        "features": features,
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:EPSG::4326"
            }
        }
    } 


def _ci_equals(column: str, param: str) -> str:
    return f"LOWER({column}) = LOWER(:{param})"

def row_to_feature(row):
    """Converte uma linha do banco para uma feature GeoJSON, removendo coordenadas 3D se existirem"""
    try:
        geometry = json.loads(row['geom_json'])       
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                k: v for k, v in row.items() 
                if k != 'geom_json' and v is not None
            }
        }
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Erro ao processar feature: {e}")
        return None

@app.get("/assentamentos_municipios")
def listar_municipios_assentamentos(_: dict = Depends(verify_token)):
    """Lista todos os municípios que possuem assentamentos estaduais."""
    sql = f"""
        SELECT DISTINCT nome_municipio
        FROM {settings.TABLE_DADOS_ASSENTAMENTOS}
        WHERE nome_municipio IS NOT NULL
        ORDER BY nome_municipio
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {"municipios": [r[0] for r in rows]}

@app.get("/geojson_reservatorios")
def geojson_reservatorios(
    municipio: str = Query("todos", description="Filtrar por município ('todos' pra geral)"),
    tolerance: Optional[float] = Query(0.001, description="Tolerância de simplificação (opc.)"),
    decimals: Optional[int] = Query(4, description="Casas decimais na geometria (opc.)"),
    _: dict = Depends(verify_token)
):
    """
    Retorna reservatórios em GeoJSON, usando o WKT em `wkt_geom`.
    """
    props = [
        "id_sagreh", "nome", "proprietario", "gerencia", "reg_hidrog",
        "nome_municipio", "nome_municipio_original","ini_monito", "ano_constr", "o_barrad",
        "ac_jusante", "id_ac_jus", "area_ha", "capacid_m3",
        "cot_vert_m", "lg_vert_m", "cot_td_m", "tipo_verte","ri"
    ]
    cols = ", ".join(f'"{c}"' for c in props)

    # Fonte de geometria: WKT
    geom_expr = "ST_GeomFromText(wkt_geom, 4326)"
    if tolerance is not None:
        geom_expr = f"ST_SimplifyPreserveTopology({geom_expr}, {tolerance})"

    # Gera GeoJSON
    if decimals is not None:
        geojson_expr = f"ST_AsGeoJSON({geom_expr}, {decimals})"
    else:
        geojson_expr = f"ST_AsGeoJSON({geom_expr})"

    sql = f"""
    SELECT
        {geojson_expr} AS geom_json,
        {cols}
      FROM {settings.TABLE_DADOS_RESERVATORIOS}
    """
    params = {}
    if municipio.lower() != "todos":
        sql += " WHERE " + _ci_equals("nome_municipio", "municipio")
        params["municipio"] = municipio

    try:
        with get_engine().connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
    except Exception as e:
        logger.error("Erro geojson_reservatorios: %s", e)
        raise HTTPException(500, "Erro ao consultar GeoJSON")

    features = []
    for row in rows:
        geom_json = row.get("geom_json")
        if not geom_json:
            continue
        try:
            features.append({
                "type": "Feature",
                "geometry": json.loads(geom_json),
                "properties": {k: row[k] for k in props}
            })
        except Exception as e:
            logger.warning("Feature inválida ignorada: %s", e)
            continue

    if not features and municipio.lower() != "todos":
        raise HTTPException(404, f"Nenhum reservatório para '{municipio}'")

    return {
        "type": "FeatureCollection",
        "features": features,
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}
        }
    }

@app.get("/reservatorios_municipios")
def listar_municipios_reservatorios(_: dict = Depends(verify_token)):
    """Lista municípios que têm reservatórios (coluna nome_municipio)."""
    sql = f"""
      SELECT DISTINCT nome_municipio
        FROM {settings.TABLE_DADOS_RESERVATORIOS}
       WHERE nome_municipio IS NOT NULL
    ORDER BY nome_municipio
    """
    try:
        with get_engine().connect() as conn:
            municipios = conn.execute(text(sql)).scalars().all()
    except Exception as e:
        logger.error("Erro listar_municipios_reservatorios: %s", e)
        raise HTTPException(500, "Erro ao listar municípios")

    return {"municipios": municipios}
