#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# import_data_to_postgres_neo.py

import os
import unicodedata
import logging

import pandas as pd
import geopandas as gpd
import numpy as np
from shapely import wkb 
from shapely.geometry import MultiPolygon


from sqlalchemy import create_engine
from geoalchemy2 import Geometry

from config import settings

### O sistema das coordenadas geográficas 
### é baseado no EPSG: 31984 - SIRGAS 2000 / UTM zone 24S


# configura logging básico
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# paths dos dados
PATH_CSV_MALHA_FUNDIARIA = "data/dataset-malha-fundiaria-idace_preprocessado-2025-08-20.csv" 
PATH_GEOJSON_MUNICIPIOS = "data/geojson-municipios_ceara-normalizado.geojson"


def get_engine():
    """Cria engine SQLAlchemy usando DSN do settings."""
    dsn = settings.postgres_dsn
    return create_engine(dsn)


def import_malha_fundiaria(csv_path: str, engine=None):
    """
    Lê CSV de malha fundiária, processa e envia para PostGIS.
    """
    if not os.path.isfile(csv_path):
        logger.error("CSV não encontrado: %s", csv_path)
        return

    # 1) Leitura em DataFrame
    df = pd.read_csv(csv_path, low_memory=False)

    # 2) Verifica colunas obrigatórias
    obrigatorias = ["modulo_fiscal", "geometry", "nome_municipio", "nome_proprietario", "regiao_administrativa", "lote_id", "numero_lote"]
    faltantes = [c for c in obrigatorias if c not in df.columns]
    if faltantes:
        logger.error("Colunas faltantes: %s", faltantes)
        return

    # 3) Conversão de tipos numéricos
    df["modulo_fiscal"] = pd.to_numeric(df["modulo_fiscal"], errors="coerce")

    # 4) Converte WKB hexadecimal para geometria
    df = df.dropna(subset=["geometry"]).copy()
    df["geometry"] = df["geometry"].apply(lambda s: wkb.loads(bytes.fromhex(s)))

    # 5) Monta GeoDataFrame com CRS original
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:31984")

    # 6) Cálculo de área a partir da geometria
    gdf["area"] = gdf.geometry.area / 10000.0  # Converte m² para hectares

    # 7) Classificação por tamanho
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

    # 8) Normaliza nome do município
    gdf["nome_municipio_original"] = gdf["nome_municipio"].str.title()
    gdf["nome_municipio"] = (
        gdf["nome_municipio"].astype(str)
        .apply(lambda s: unicodedata.normalize("NFKD", s)
               .encode("ASCII", "ignore").decode()
               .lower().replace(" ", "_"))
    )

    # 9) Persistência no PostGIS com geometria original (31984)
    eng = engine or get_engine()
    logger.info("Importando %s para tabela '%s'...", csv_path, settings.TABLE_DADOS_FUNDIARIOS)
    gdf.to_postgis(
        settings.TABLE_DADOS_FUNDIARIOS,
        con=eng,
        if_exists='replace',
        index=False,
        dtype={
            "geometry": Geometry("MULTIPOLYGON", srid=31984)
        }
    )
    logger.info("✔️ Importação de %s concluída", settings.TABLE_DADOS_FUNDIARIOS)
    return len(gdf)


def import_municipios(geojson_path: str, engine=None):
    """
    Lê GeoJSON de municípios e envia para PostGIS.
    """
    if not os.path.isfile(geojson_path):
        logger.error("GeoJSON não encontrado: %s", geojson_path)
        return

    # Lê e mantém CRS original
    gdf = gpd.read_file(geojson_path)
    
    # Converte nomes de colunas para minúsculo
    gdf.columns = [col.lower() for col in gdf.columns]

    eng = engine or get_engine()
    logger.info("Importando %s para tabela '%s'...", geojson_path, settings.TABLE_GEOM_MUNICIPIOS)
    gdf.to_postgis(
        settings.TABLE_GEOM_MUNICIPIOS,
        con=eng,
        if_exists='replace',
        index=False,
        dtype={
            "geometry": Geometry("MULTIPOLYGON", srid=gdf.crs.to_epsg() if gdf.crs else 4326)
        }
    )
    logger.info("✔️ Importação de %s concluída", settings.TABLE_GEOM_MUNICIPIOS)
    return len(gdf)


def main():
    logger.info("Iniciando importações fundiárias e de municípios...")
    eng = get_engine()
    quantidade_de_lotes = import_malha_fundiaria(PATH_CSV_MALHA_FUNDIARIA, engine=eng)
    logger.info(f"Foram importados {quantidade_de_lotes} lotes!")
    quantidade_de_municipios = import_municipios(PATH_GEOJSON_MUNICIPIOS, engine=eng)
    logger.info(f"Foram importados {quantidade_de_municipios} municípios!")
    logger.info("Todas as importações concluídas com sucesso!")


if __name__ == '__main__':
    main()