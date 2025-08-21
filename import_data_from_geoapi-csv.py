#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# import_data_from_geoapi-csv.py

import os
import logging

import pandas as pd
import geopandas as gpd
import numpy as np

from shapely import wkb # Novo: para ler WKB
from shapely.geometry import MultiPolygon, Polygon # Novo: para tipagem


from sqlalchemy import create_engine
from geoalchemy2 import Geometry

from config import settings





# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Constantes
TABLE_MALHA_FUNDIARIA = "malha_fundiaria_ceara"
TABLE_MUNICIPIOS = "municipios_ceara"
SRID = 4326  # WGS 84 (confirmado)

PATH_CSV_MALHA_FUNDIARIA = "data/dataset-malha-fundiaria-idace_preprocessado-2025-08-17.csv"
PATH_GEOJSON_MUNICIPIOS = "data/geojson-municipios_ceara-normalizado.geojson"

def get_engine():
    """Cria engine SQLAlchemy usando DSN do settings."""
    return create_engine(settings.postgres_dsn)

def parse_wkb(val):
    """
    Converte um valor vindo do CSV (hex string ou bytes) em geometria Shapely.
    Aceita:
      - str: WKB em hex
      - bytes/bytearray/memoryview: WKB em binário
      - geometria Shapely (retorna direto)
    Retorna None para NaN.
    """
    if pd.isna(val):
        return None

    # Já é geometria Shapely?
    try:
        from shapely.geometry.base import BaseGeometry
        if isinstance(val, BaseGeometry):
            return val
    except Exception:
        pass

    # Converte de bytes ou hex string
    try:
        if isinstance(val, (bytes, bytearray, memoryview)):
            return wkb.loads(val)
        if isinstance(val, str):
            return wkb.loads(val, hex=True)
    except Exception as e:
        logger.warning(f"Falha ao converter geometria: {e}")
        return None

    raise ValueError(f"Tipo de geometria não suportado: {type(val)}")

def ensure_multipolygon(geom):
    """Converte Polygon para MultiPolygon se necessário."""
    if geom is None:
        return None
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    return geom

def import_malha_fundiaria(csv_path: str, engine=None):
    """
    Importa dados fundiários de um CSV com geometrias em WKB (EPSG:4326).
    """
    if not os.path.isfile(csv_path):
        logger.error("Arquivo CSV não encontrado: %s", csv_path)
        return None

    try:
        # 1. Leitura do CSV
        df = pd.read_csv(csv_path, low_memory=False)
        
        # 2. Validação das colunas obrigatórias
        required_cols = [
            "modulo_fiscal",
            "area",
            "geometry",
            "nome_municipio",
            "nome_proprietario",
            "regiao_administrativa"
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error("Colunas obrigatórias faltando: %s", missing_cols)
            return None

        # 3. Conversão de tipos numéricos
        numeric_cols = ["modulo_fiscal", "area", "perimetro", "perimetro_km"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 4. Processamento da geometria
        df = df.dropna(subset=["geometry"]).copy()
        df["geometry"] = df["geometry"].apply(parse_wkb)
        df = df.dropna(subset=["geometry"]).copy()
        df["geometry"] = df["geometry"].apply(ensure_multipolygon)

        # 5. Criação do GeoDataFrame com CRS 4326
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=f"EPSG:{SRID}")

        # 6. Importação para o PostGIS
        engine = engine or get_engine()
        logger.info("Importando %d registros para %s", len(gdf), TABLE_MALHA_FUNDIARIA)
        
        gdf.to_postgis(
            TABLE_MALHA_FUNDIARIA,
            con=engine,
            if_exists='replace',
            index=False,
            dtype={"geometry": Geometry("MULTIPOLYGON", srid=SRID)}
        )
        
        logger.info("Importação da malha fundiária concluída com sucesso")
        return len(gdf)
        
    except Exception as e:
        logger.error("Erro na importação da malha fundiária: %s", str(e))
        return None

def import_municipios(geojson_path: str, engine=None):
    """
    Importa limites municipais de um GeoJSON para o PostGIS (EPSG:4326).
    """
    if not os.path.isfile(geojson_path):
        logger.error("Arquivo GeoJSON não encontrado: %s", geojson_path)
        return None

    try:
        # 1. Leitura e normalização do GeoJSON
        gdf = gpd.read_file(geojson_path)
        gdf = gdf.to_crs(epsg=SRID)
        gdf.columns = [col.lower() for col in gdf.columns]

        # 2. Importação para o PostGIS
        engine = engine or get_engine()
        logger.info("Importando %d municípios para %s", len(gdf), TABLE_MUNICIPIOS)
        
        gdf.to_postgis(
            TABLE_MUNICIPIOS,
            con=engine,
            if_exists='replace',
            index=False,
            dtype={"geometry": Geometry("MULTIPOLYGON", srid=SRID)}
        )
        
        logger.info("Importação de municípios concluída com sucesso")
        return len(gdf)
        
    except Exception as e:
        logger.error("Erro na importação de municípios: %s", str(e))
        return None

def main():
    """Função principal para execução do script."""
    logger.info("Iniciando processo de importação de dados geoespaciais...")
    
    engine = get_engine()
    
    # Importação dos dados fundiários
    qtd_lotes = import_malha_fundiaria(PATH_CSV_MALHA_FUNDIARIA, engine)
    if qtd_lotes:
        logger.info("Lotes importados: %d", qtd_lotes)
    
    # Importação dos municípios
    qtd_mun = import_municipios(PATH_GEOJSON_MUNICIPIOS, engine)
    if qtd_mun:
        logger.info("Municípios importados: %d", qtd_mun)
    
    logger.info("Processo concluído! ✅")

if __name__ == '__main__':
    main()