#!/usr/bin/env python3

import os
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd
from shapely import wkt
from unidecode import unidecode
from sqlalchemy import create_engine
from geoalchemy2 import Geometry


from config import settings # Carregar variáveis de ambiente do arquivo .env

# Variáveis de ambiente para conexão e tabela
db_type    = settings.DATABASE_TYPE
user       = settings.POSTGRES_USER
password   = settings.POSTGRES_PASSWORD
host       = settings.POSTGRES_HOST
port       = settings.POSTGRES_PORT
db_name    = settings.POSTGRES_DB
table_name = settings.TABLE_DADOS_RESERVATORIOS

# Construir string de conexão
connection_str = f"{db_type}ql://{user}:{password}@{host}:{port}/{db_name}"
engine = create_engine(connection_str)

# Caminho para o CSV
csv_path = '../datasets/reservatorios_ceara.csv'

print("Lendo CSV...")
df = pd.read_csv(csv_path)

# Normalizar nomes de município
print("Normalizando nomes de município...")
df['nome_municipio_original'] = df['municipio']
df['nome_municipio'] = (
    df['nome_municipio_original']
    .astype(str)
    .apply(lambda x: unidecode(x.lower()).replace(' ', '_'))
)
# Remover coluna original
df = df.drop(columns=['municipio'], errors='ignore')

# Converter coluna WKT para geometria
print("Convertendo WKT para geometria...")
df['geometry'] = df['wkt'].apply(wkt.loads)

# Criar GeoDataFrame com CRS EPSG:4326
gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')


print("Calculando área em hectares... (projetando para EPSG:3857)...")
area_m2 = gdf.to_crs(epsg=3857)['geometry'].area
gdf['area_ha'] = area_m2 / 10000.0


gdf = gdf.rename(columns={'wkt': 'wkt_geom'})
gdf = gdf.rename(columns={'proprietar': 'proprietario'})


print(f"Enviando para o PostGIS... (tabela: {table_name})")
gdf.to_postgis(
    name=table_name,
    con=engine,
    if_exists='replace',
    index=False,
    dtype={'geometry': Geometry('GEOMETRY', srid=4326)}
)

# Resumo
do_importados = len(gdf)
print(f"Importação concluída! {do_importados} registros inseridos na tabela {table_name}.")
