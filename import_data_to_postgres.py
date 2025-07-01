#!/usr/bin/env python3

# import_data_to_postgresa.py

import os
import subprocess
import tempfile
import pandas as pd
import geopandas as gpd
import numpy as np
import unicodedata
from shapely import wkt
from config import settings

# Tabelas alvo no PostGIS
TABLE_MALHA_FUNDIARIA = "malha_fundiaria_ceara"
TABLE_MUNICIPIOS = "municipios_ceara"


def ogr2ogr_to_db(input_path: str, layer_name: str, input_format: str, force_4326: bool = False):
    """
    Usa ogr2ogr para importar arquivo para PostGIS, opcionalmente reprojetando para EPSG:4326.
    """
    db_dsn = settings.postgres_dsn
    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        db_dsn,
        input_path,
        "-nln", layer_name,
        "-overwrite"
    ]
    if force_4326:
        cmd.extend(["-t_srs", "EPSG:4326"])

    if input_format.lower() == "csv":
        cmd.extend(["-oo", "GEOM_POSSIBLE_NAMES=geom"])

    print(f"Importando {input_path} → tabela '{layer_name}'...")
    subprocess.run(cmd, check=True)
    print(f"✔️  Importação de '{layer_name}' concluída")


def import_malha_fundiaria(csv_path: str):
    """
    Lê CSV, limpa, converte WKT, reprojeta para 4326, classifica e importa para PostGIS.
    """
    if not os.path.isfile(csv_path):
        print(f"✗ CSV não encontrado: {csv_path}")
        return

    # 1) Leitura
    df = pd.read_csv(csv_path, low_memory=False)

    # 2) Verifica colunas obrigatórias
    obrig = ["modulo_fiscal", "area", "geom", "nome_municipio", "regiao_administrativa"]
    for col in obrig:
        if col not in df.columns:
            print(f"✗ Coluna obrigatória '{col}' ausente")
            return

    # 3) Conversão de tipos
    df["modulo_fiscal"] = df["modulo_fiscal"].astype(float)
    df["area"] = df["area"].astype(float)

    # 4) Carregar WKT textual (EPSG:31984) e converter para geometria
    df = df[df["geom"].notna()].copy()
    df["geometry"] = df["geom"].apply(lambda s: wkt.loads(s) if pd.notna(s) else None)
    df = df.dropna(subset=["geometry"]).copy()

    # 5) Criar GeoDataFrame no CRS projetado correto
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:31984")

    # Calcular área (hectares) e perímetro (quilômetros)
    gdf["area"] = gdf.geometry.area / 10000.0
    gdf["perimetro"] = gdf.geometry.length / 1000.0

    # 6) Reprojetar para WGS84 (se precisar para outras análises)
    gdf = gdf.to_crs(epsg=4326)

    # 7) Classificação por módulo fiscal com base na área já calculada
    mf = gdf["modulo_fiscal"]
    area = gdf["area"]
    conds = [
        (area > 0) & (area < mf),
        (area >= mf) & (area <= 4 * mf),
        (area > 4 * mf) & (area <= 15 * mf),
        (area > 15 * mf)
    ]
    cats = [
        "Pequena Propriedade < 1 MF",
        "Pequena Propriedade",
        "Média Propriedade",
        "Grande Propriedade"
    ]
    gdf["categoria"] = np.select(conds, cats, default="Sem Classificação")

    # 7) Normaliza nome do município
    gdf["municipio_norm"] = gdf["nome_municipio"].apply(
        lambda s: unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode().lower()
    )

    # 8) Exporta GeoJSON temporário
    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
        tmp_path = tmp.name
    gdf.to_file(tmp_path, driver="GeoJSON")

    # 9) Importa para PostGIS
    ogr2ogr_to_db(tmp_path, TABLE_MALHA_FUNDIARIA, "GeoJSON", force_4326=True)

    # 10) Remove temporário
    os.remove(tmp_path)


def import_municipios(geojson_path: str):
    """
    Importa GeoJSON de municípios para PostGIS.
    """
    if not os.path.isfile(geojson_path):
        print(f"✗ GeoJSON não encontrado: {geojson_path}")
        return

    # Usa ogr2ogr direto com reprojeção
    ogr2ogr_to_db(geojson_path, TABLE_MUNICIPIOS, "GeoJSON", force_4326=True)


def main():
    import_malha_fundiaria("data/dataset-malha-fundiaria-idace_preprocessado-2025-06-26.csv")
    import_municipios("data/geojson-municipios_ceara-normalizado.geojson")
    print("✅ Importação completa!")


if __name__ == '__main__':
    main()
