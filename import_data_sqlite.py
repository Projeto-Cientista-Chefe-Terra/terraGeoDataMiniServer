#!/usr/bin/env python3
# import_data.py

import os
import sys
import argparse
import subprocess
import tempfile

import pandas as pd
import geopandas as gpd
from shapely import wkt
import numpy as np
import unicodedata

# ---------------------------------------------------------------------------------------------------
# 1) Diretórios e arquivos
# ---------------------------------------------------------------------------------------------------
DATA_DIR = "data"
MUNI_GEOJSON = os.path.join(DATA_DIR, "geojson-municipios_ceara-normalizado.geojson")
FUNDIARIA_CSV = os.path.join(DATA_DIR, "dataset-malha-fundiaria-idace_preprocessado-2025-04-26.csv")

# Nome do arquivo .sqlite que conterá SpatiaLite
SQLITE_DB = os.path.join(DATA_DIR, "terra_data.sqlite")

# Tabelas dentro do SQLite/SpatiaLite
TABLE_MUNICIPIOS = "municipios_ceara"
TABLE_FUNDOS      = "malha_fundiaria_ceara"

# ---------------------------------------------------------------------------------------------------
# 2) Função auxiliar para chamar ogr2ogr e criar/substituir tabela em SpatiaLite
# ---------------------------------------------------------------------------------------------------
def ogr2ogr_to_spatialite(
    sqlite_path: str,
    input_path: str,
    layer_name: str,
    input_format: str
):
    """
    Invoca ogr2ogr para copiar `input_path` (GeoJSON, CSV, etc.) para o banco `sqlite_path`,
    criando ou substituindo a tabela `layer_name` em formato SpatiaLite.
    """
    # Comando base:
    # ogr2ogr -f SQLite -dsco SPATIALITE=YES terra_data.sqlite <input> -nln <layer_name> -overwrite
    cmd = [
        "ogr2ogr",
        "-f", "SQLite",
        "-dsco", "SPATIALITE=YES",
        sqlite_path,
        input_path,
        "-nln", layer_name,
        "-overwrite"
    ]

    # Se o input for CSV, adiciona opção para ler coluna WKT "geom"
    if input_format.lower() == "csv":
        # Inserir: -oo GEOM_POSSIBLE_NAMES=geom
        cmd.insert(5, "-oo")
        cmd.insert(6, "GEOM_POSSIBLE_NAMES=geom")

    try:
        print(f"    ↪ executando: {' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            print("✗ ogr2ogr retornou erro:")
            print(proc.stderr.strip())
            return False
        return True
    except FileNotFoundError:
        print("✗ Comando 'ogr2ogr' não encontrado. Verifique se GDAL/OGR está instalado e no PATH.")
        return False
    except Exception as e:
        print(f"✗ Erro ao chamar ogr2ogr: {e}")
        return False


# ---------------------------------------------------------------------------------------------------
# 3) Importando municípios do Ceará para SpatiaLite
# ---------------------------------------------------------------------------------------------------
def import_municipios(geojson_path: str):
    print(f"\n[→] Importando municípios do Ceará: '{geojson_path}' → tabela '{TABLE_MUNICIPIOS}'")

    if not os.path.isfile(geojson_path):
        print(f"✗ Arquivo GeoJSON não encontrado: {geojson_path}")
        return

    # Se o banco já existe, removemos para recriar do zero
    if os.path.isfile(SQLITE_DB):
        try:
            os.remove(SQLITE_DB)
        except Exception as e:
            print(f"✗ Erro ao remover banco existente: {e}")
            return

    # Chama ogr2ogr para criar tabela de municípios
    ok = ogr2ogr_to_spatialite(
        sqlite_path=SQLITE_DB,
        input_path=geojson_path,
        layer_name=TABLE_MUNICIPIOS,
        input_format="GeoJSON"
    )
    if ok:
        print(f"✔ Municípios gravados em '{TABLE_MUNICIPIOS}' com sucesso.")
    else:
        print("✗ Falha ao gravar municípios em SpatiaLite.")


# ---------------------------------------------------------------------------------------------------
# 4) Importando malha fundiária para SpatiaLite
# ---------------------------------------------------------------------------------------------------
def import_malha_fundiaria(csv_path: str):
    print(f"\n[→] Importando malha fundiária CSV: '{csv_path}' → tabela '{TABLE_FUNDOS}'")

    if not os.path.isfile(csv_path):
        print(f"✗ CSV não encontrado: {csv_path}")
        return

    # 4.1. Lê o CSV no Pandas
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        print(f"✗ Erro ao ler CSV da malha fundiária: {e}")
        return

    obrigatorias = ["modulo_fiscal", "area", "geom", "nome_municipio", "regiao_administrativa"]
    for col in obrigatorias:
        if col not in df.columns:
            print(f"✗ Coluna obrigatória '{col}' não encontrada no CSV.")
            return

    # 4.2. Converte tipos
    df["modulo_fiscal"] = df["modulo_fiscal"].astype(float)
    df["area"] = df["area"].astype(float)

    # 4.3. Filtra linhas sem WKT
    df = df[df["geom"].notna()].copy()
    if df.empty:
        print("⚠️  Nenhuma geometria WKT válida no CSV.")
        return

    # 4.4. Converte WKT → Shapely
    try:
        df["geometry"] = df["geom"].apply(lambda s: wkt.loads(s) if pd.notna(s) else None)
    except Exception as e:
        print(f"✗ Erro ao converter WKT em Shapely: {e}")
        return

    df = df.dropna(subset=["geometry"]).copy()
    if df.empty:
        print("⚠️  Todas as linhas tinham geometria inválida.")
        return

    # 4.5. Cria GeoDataFrame e define CRS origem (EPSG:31984) → projeta para 4326
    try:
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:31984")
        gdf = gdf.to_crs(epsg=4326)
    except Exception as e:
        print(f"✗ Erro ao setar/projetar CRS: {e}")
        return

    # 4.6. Classificação de categoria
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

    # 4.7. Normaliza nome do município
    gdf["municipio_norm"] = gdf["nome_municipio"].apply(
        lambda s: unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode().lower()
    )

    # 4.8. Exporta para GeoJSON temporário
    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
        tmp_geojson = tmp.name

    try:
        gdf.to_file(tmp_geojson, driver="GeoJSON")
    except Exception as e:
        print(f"✗ Erro ao gravar GeoJSON temporário: {e}")
        return

    # 4.9. Chama ogr2ogr para inserir no SpatiaLite
    ok = ogr2ogr_to_spatialite(
        sqlite_path=SQLITE_DB,
        input_path=tmp_geojson,
        layer_name=TABLE_FUNDOS,
        input_format="GeoJSON"
    )

    # Remove o arquivo temporário
    try:
        os.remove(tmp_geojson)
    except Exception:
        pass

    if ok:
        print(f"✔ Malha fundiária gravada em '{TABLE_FUNDOS}' com sucesso.")
    else:
        print("✗ Falha ao gravar malha fundiária em SpatiaLite.")


# ---------------------------------------------------------------------------------------------------
# 5) Função principal: parse de args e chamadas
# ---------------------------------------------------------------------------------------------------
def main():
    # Neste ponto, DECLARE global antes de qualquer uso:
    global SQLITE_DB, MUNI_GEOJSON, FUNDIARIA_CSV

    parser = argparse.ArgumentParser(description="Importa municípios e malha fundiária no SQLite/SpatiaLite")
    parser.add_argument(
        "--muni-geojson", "-m",
        default=MUNI_GEOJSON,
        help="Caminho para o GeoJSON dos municípios do Ceará"
    )
    parser.add_argument(
        "--fundiaria-csv", "-f",
        default=FUNDIARIA_CSV,
        help="Caminho para o CSV da malha fundiária"
    )
    parser.add_argument(
        "--sqlite-db", "-s",
        default=SQLITE_DB,
        help="Arquivo SQLite/SpatiaLite a criar/usar"
    )
    args = parser.parse_args()

    # Agora podemos sobrescrever as variáveis globais:
    SQLITE_DB     = args.sqlite_db
    MUNI_GEOJSON  = args.muni_geojson
    FUNDIARIA_CSV = args.fundiaria_csv

    # Verifica existência dos arquivos de entrada
    if not os.path.isfile(MUNI_GEOJSON):
        print(f"✗ GeoJSON não encontrado: {MUNI_GEOJSON}")
        sys.exit(1)
    if not os.path.isfile(FUNDIARIA_CSV):
        print(f"✗ CSV não encontrado: {FUNDIARIA_CSV}")
        sys.exit(1)

    # Chama as funções de importação
    import_municipios(MUNI_GEOJSON)
    import_malha_fundiaria(FUNDIARIA_CSV)

    print("\n✅ Importação concluída. Banco disponível em:", SQLITE_DB)


if __name__ == "__main__":
    main()