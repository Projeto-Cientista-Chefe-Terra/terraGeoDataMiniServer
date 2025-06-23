#!/usr/bin/env python3

import os
import subprocess
import tempfile
import pandas as pd
import numpy as np
import unicodedata
import geopandas as gpd
from shapely import wkt
from config import settings

# Tabela alvo no PostGIS
TABLE_ASSENTAMENTOS = "assentamentos_estaduais_ceara"

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

def normalize_municipio_name(name: str) -> str:
    """
    Normaliza o nome do município: minúsculas, sem acentos, espaços substituídos por _
    """
    if pd.isna(name):
        return ""
    
    # Remove acentos e caracteres especiais
    normalized = unicodedata.normalize('NFKD', str(name))
    normalized = normalized.encode('ASCII', 'ignore').decode('ASCII')
    
    # Converte para minúsculas e substitui espaços por _
    normalized = normalized.lower().strip().replace(" ", "_")
    
    # Remove caracteres inválidos
    normalized = ''.join(c for c in normalized if c.isalnum() or c == '_')
    
    return normalized

def calculate_area(geom_wkt: str) -> float:
    """
    Calcula a área em hectares a partir da geometria WKT
    """
    try:
        geom = wkt.loads(geom_wkt)
        return geom.area / 10000  # Convertendo m² para hectares
    except:
        return None

def process_assentamentos_data(csv_path: str):
    """
    Processa o arquivo CSV de assentamentos e prepara para importação.
    """
    if not os.path.isfile(csv_path):
        print(f"✗ CSV não encontrado: {csv_path}")
        return

    # 1) Leitura do CSV
    df = pd.read_csv(csv_path, low_memory=False)

    # 2) Verifica colunas obrigatórias
    required_cols = ["Name", "wkt_geom"]
    for col in required_cols:
        if col not in df.columns:
            print(f"✗ Coluna obrigatória '{col}' ausente")
            return

    # 3) Processa a coluna Name
    df[['nome_municipio_original', 'nome_assentamento']] = df['Name'].str.split('-', n=1, expand=True)
    
    # Limpeza dos nomes
    df['nome_municipio_original'] = df['nome_municipio_original'].str.strip()
    df['nome_assentamento'] = df['nome_assentamento'].str.strip().str.lower()
    
    # Cria coluna normalizada
    df['nome_municipio'] = df['nome_municipio_original'].apply(normalize_municipio_name)

    # 4) Calcula a área a partir da geometria
    df['area'] = df['wkt_geom'].apply(calculate_area)

    # 5) Seleciona apenas as colunas que serão importadas
    df = df[['nome_municipio', 'nome_assentamento', 'nome_municipio_original', 'area', 'wkt_geom']].copy()

    # 6) Renomeia a coluna de geometria para 'geom' (padrão PostGIS)
    df = df.rename(columns={'wkt_geom': 'geom'})

    # 7) Remove linhas sem geometria válida
    df = df[df['geom'].notna()].copy()

    # 8) Cria GeoDataFrame com a geometria
    gdf = gpd.GeoDataFrame(
        df.drop('geom', axis=1),
        geometry=df['geom'].apply(wkt.loads),
        crs="EPSG:4326"
    )

    # 9) Exporta para GeoJSON temporário
    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
        tmp_path = tmp.name
    
    gdf.to_file(tmp_path, driver='GeoJSON')

    # 10) Importa para PostGIS
    ogr2ogr_to_db(tmp_path, TABLE_ASSENTAMENTOS, "GeoJSON", force_4326=False)

    # 11) Remove arquivo temporário
    os.remove(tmp_path)

def main():
    # Processa os dados tabulares do CSV
    process_assentamentos_data("data/assentamentos estaduais_Idace 2025_corrigidosporccterra.csv")
    print("✅ Importação completa!")

if __name__ == '__main__':
    main()