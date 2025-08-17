
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# import_data_to_postgresa.py

import os
import tempfile
import unicodedata
import logging

import pandas as pd
import geopandas as gpd
import numpy as np
from shapely import wkt
from sqlalchemy import create_engine
from geoalchemy2 import Geometry

from config import settings

# configura logging básico
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# tabelas alvo no PostGIS
TABLE_MALHA_FUNDIARIA = "malha_fundiaria_ceara"
TABLE_MUNICIPIOS = "municipios_ceara"

# paths dos dados
PATH_CSV_MALHA_FUNDIARIA = "data/dataset-malha-fundiaria-idace_preprocessado-2025-08-17.csv" 
PATH_GEOJSON_MUNICIPIOS = "data/geojson-municipios_ceara-normalizado.geojson"


def get_engine():
    """Cria engine SQLAlchemy usando DSN do settings."""
    dsn = settings.postgres_dsn  # ex: 'postgresql+psycopg2://user:pw@host:port/db'
    return create_engine(dsn)


def import_malha_fundiaria(csv_path: str, engine=None):
    """
    Lê CSV de malha fundiária, processa e envia para PostGIS via to_postgis.
    """
    if not os.path.isfile(csv_path):
        logger.error("CSV não encontrado: %s", csv_path)
        return

    # 1) Leitura em DataFrame
    df = pd.read_csv(csv_path, low_memory=False)

    # 2) Verifica colunas obrigatórias
    obrigatorias = ["modulo_fiscal", "area", "geom", "nome_municipio", "nome_proprietario","regiao_administrativa"]
    faltantes = [c for c in obrigatorias if c not in df.columns]
    if faltantes:
        logger.error("Colunas faltantes: %s", faltantes)
        return

    # 3) Conversão de tipos numéricos
    df["modulo_fiscal"] = pd.to_numeric(df["modulo_fiscal"], errors="coerce")
    df["area"] = pd.to_numeric(df["area"], errors="coerce")

    # 4) Converte WKT para geometria
    df = df.dropna(subset=["geom"]).copy()
    df["geometry"] = df["geom"].apply(lambda s: wkt.loads(s))

    # 5) Monta GeoDataFrame e configura CRS original
    gdf = gpd.GeoDataFrame(
        df.drop(columns=["geom"]),
        geometry="geometry",
        crs="EPSG:31984"
    )

    # 6) Cálculo de métricas
    gdf["area"] = gdf.geometry.area / 10000.0
    gdf["perimetro_km"] = gdf.geometry.length / 1000.0

    # 7) Reprojeção para WGS84
    gdf = gdf.to_crs(epsg=4326)

    # 8) Classificação por tamanho
    mf = gdf["modulo_fiscal"]
    ha = gdf["area"]
    conds = [
        (ha > 0) & (ha < mf),
        (ha >= mf) & (ha <= 4 * mf),
        (ha > 4 * mf) & (ha <= 15 * mf),
        (ha > 15 * mf)
    ]
    cats = [
        "Pequena Propriedade < 1 MF",
        "Pequena Propriedade",
        "Média Propriedade",
        "Grande Propriedade"
    ]
    gdf["categoria"] = np.select(conds, cats, default="Sem Classificação")

    # 9) Normaliza nome do município
    gdf["nome_municipio_original"] = gdf["nome_municipio"].str.title()
    gdf["nome_municipio"] = (
        gdf["nome_municipio"].astype(str)
        .apply(lambda s: unicodedata.normalize("NFKD", s)
               .encode("ASCII", "ignore").decode()
               .lower().replace(" ", "_"))
    )

    # 10) Persistência no PostGIS
    eng = engine or get_engine()
    logger.info("Importando %s para tabela '%s'...", csv_path, TABLE_MALHA_FUNDIARIA)
    gdf.to_postgis(
        TABLE_MALHA_FUNDIARIA,
        con=eng,
        if_exists='replace',
        index=False,
        dtype={
            "geometry": Geometry("MULTIPOLYGON", srid=4326)
        }
    )
    logger.info("✔️ Importação de %s concluída", TABLE_MALHA_FUNDIARIA)
    return len(gdf)


def import_municipios(geojson_path: str, engine=None):
    """
    Lê GeoJSON de municípios e envia para PostGIS com colunas em minúsculo.
    """
    if not os.path.isfile(geojson_path):
        logger.error("GeoJSON não encontrado: %s", geojson_path)
        return

    # lê e padroniza CRS
    gdf = gpd.read_file(geojson_path)
    gdf = gdf.to_crs(epsg=4326)

    # força todos os nomes de coluna a serem minusculos
    gdf.columns = [col.lower() for col in gdf.columns]

    eng = engine or get_engine()
    logger.info("Importando %s para tabela '%s'...", geojson_path, TABLE_MUNICIPIOS)
    gdf.to_postgis(
        TABLE_MUNICIPIOS,
        con=eng,
        if_exists='replace',
        index=False,
        dtype={
            "geometry": Geometry("MULTIPOLYGON", srid=4326)
        }
    )
    logger.info("✔️ Importação de %s concluída", TABLE_MUNICIPIOS)
    return len(gdf)



def main():
    logger.info("Iniciando importações fundiárias e de municípios...")
    eng = get_engine()
    quantidade_de_lotes = import_malha_fundiaria(PATH_CSV_MALHA_FUNDIARIA, engine=eng)
    logger.info(f"Foram importados {quantidade_de_lotes} lotes!")
    quantidade_de_municipios = import_municipios(PATH_GEOJSON_MUNICIPIOS, engine=eng)
    logger.info(f"Foram importados {quantidade_de_municipios} lotes!")
    logger.info("Todas as importações concluídas com sucesso!")


if __name__ == '__main__':
    main()
