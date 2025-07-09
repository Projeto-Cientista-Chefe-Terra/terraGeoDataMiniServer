#!/usr/bin/env python3

import os
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd
from shapely import wkt
from unidecode import unidecode
from sqlalchemy import create_engine
from geoalchemy2 import Geometry

# Carregar variáveis de ambiente do arquivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Variáveis de ambiente para conexão e tabela
db_type    = os.getenv('DATABASE_TYPE', 'postgres')
user       = os.getenv('POSTGRES_USER', 'seu_usuario')
password   = os.getenv('POSTGRES_PASSWORD', 'sua_senha')
host       = os.getenv('POSTGRES_HOST', 'localhost')
port       = os.getenv('POSTGRES_PORT', '5432')
db_name    = os.getenv('POSTGRES_DB', 'seu_banco')
table_name = os.getenv('TABLE_DADOS_RESERVATORIOS', 'reservatorios_monitorados')

# Construir string de conexão
connection_str = f"{db_type}ql://{user}:{password}@{host}:{port}/{db_name}"
engine = create_engine(connection_str)

# Caminho para o CSV
csv_path = 'data/reservatorios-monitorados-2025-07-08.csv'

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
