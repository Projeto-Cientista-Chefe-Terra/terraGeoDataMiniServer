#!/usr/bin/env python3

import os
import psycopg2
import pandas as pd
import numpy as np
from unidecode import unidecode
from dotenv import load_dotenv
from shapely import wkt
from shapely.ops import transform
from pyproj import Transformer

load_dotenv()

# Reprojetar do WGS84 para UTM zona 24S (EPSG:4326 -> EPSG:31984)
transformer = Transformer.from_crs(4326, 31984, always_xy=True)

def validar_coordenada(valor):
    if pd.isna(valor) or valor in (None, np.nan, ''):
        return None
    try:
        valor_float = float(valor)
        if np.isinf(valor_float) or np.isnan(valor_float):
            return None
        return valor_float
    except (ValueError, TypeError):
        return None

def calcular_centroid_wkt(wkt_geom):
    try:
        geom = wkt.loads(wkt_geom)
        centroid = geom.centroid
        return centroid.x, centroid.y
    except Exception:
        return None, None

def calcular_area_ha_wkt(wkt_geom):
    try:
        poly = wkt.loads(wkt_geom)
        # reprojeta cada ponto do polígono de EPSG:4326 para EPSG:31984
        poly_proj = transform(transformer.transform, poly)
        area_m2 = poly_proj.area
        return area_m2 / 10000  # converte m² para ha
    except Exception:
        return None

def importar_reservatorios():
    db_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'port': os.getenv('POSTGRES_PORT')
    }
    
    table_name = os.getenv('TABLE_DADOS_RESERVATORIOS')
    csv_path = "data/reservatorios-monitorados-2025-07-08.csv"
    
    try:
        df = pd.read_csv(
            csv_path,
            encoding='utf-8',
            delimiter=',',
            na_values=['', 'NA', 'N/A', 'NaN', 'None', 'inf', '-inf', 'INF', '-INF']
        )
        
        # Ajusta coluna do polígono WKT
        if 'geom' in df.columns:
            df['wkt_geometry'] = df['geom']

        # Calcula centroid para POINT e área em hectares a partir do polígono reprojetado
        df['longitude'], df['latitude'] = zip(*df['wkt_geometry'].map(calcular_centroid_wkt))
        df['area_calculada'] = df['wkt_geometry'].map(calcular_area_ha_wkt)

        # Cria campo 'geom' para banco (POINT)
        df['geom_point_wkt'] = df.apply(
            lambda row: f"POINT({row['longitude']} {row['latitude']})"
            if validar_coordenada(row['longitude']) and validar_coordenada(row['latitude'])
            else None, axis=1
        )

        # Conversões de tipos
        df['id_sagreh'] = pd.to_numeric(df.get('id_sagreh', None), errors='coerce').fillna(0).astype(int)
        df['id_ac_jus'] = pd.to_numeric(df.get('id_ac_jus', None), errors='coerce').fillna(0).astype(int)
        df['ano_constr'] = pd.to_numeric(df.get('ano_constr', None), errors='coerce').fillna(0).astype(int)

        float_cols = ['area_ha', 'capacid_m3', 'cot_vert_m', 'lg_vert_m', 'x', 'y']
        for col in float_cols:
            df[col] = pd.to_numeric(df.get(col, None), errors='coerce')

        text_cols = ['nome', 'proprietario', 'gerencia', 'reg_hidrog', 'nome_municipio',
                     'rio_barrad', 'ac_jusante', 'tipo_verte', 'cot_td_m']
        for col in text_cols:
            df[col] = df.get(col, '').astype(str).replace({'nan': None, 'None': None, '': None})
        
        # Datas
        if 'ini_monito' in df.columns:
            df['ini_monito'] = pd.to_datetime(df['ini_monito'], errors='coerce')
        else:
            df['ini_monito'] = None

        df = df.drop_duplicates(subset=['id_sagreh', 'nome'])
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                id_sagreh INTEGER,
                nome VARCHAR(100),
                proprietario VARCHAR(100),
                gerencia VARCHAR(100),
                reg_hidrog VARCHAR(100),
                nome_municipio VARCHAR(100),
                ini_monito DATE,
                ano_constr INTEGER,
                rio_barrad VARCHAR(100),
                ac_jusante VARCHAR(100),
                id_ac_jus INTEGER,
                area_ha DOUBLE PRECISION,
                capacid_m3 DOUBLE PRECISION,
                cot_vert_m DOUBLE PRECISION,
                lg_vert_m DOUBLE PRECISION,
                cot_td_m TEXT,
                tipo_verte TEXT,
                x DOUBLE PRECISION,
                y DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                wkt_geometry TEXT,
                geom GEOMETRY(POINT, 4326),
                area_calculada DOUBLE PRECISION
            );
        """)
        
        cursor.execute(f"TRUNCATE TABLE {table_name};")

        for _, row in df.iterrows():
            try:
                geom_value = f"ST_GeomFromText('{row['geom_point_wkt']}', 4326)" if row['geom_point_wkt'] else 'NULL'
                cursor.execute(f"""
                INSERT INTO {table_name} (
                    id_sagreh, nome, proprietario, gerencia, reg_hidrog, nome_municipio,
                    ini_monito, ano_constr, rio_barrad, ac_jusante, id_ac_jus,
                    area_ha, capacid_m3, cot_vert_m, lg_vert_m, cot_td_m, tipo_verte,
                    x, y, longitude, latitude, wkt_geometry, geom, area_calculada
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    {geom_value}, %s
                );
                """, (
                    row['id_sagreh'],
                    row['nome'],
                    row['proprietario'],
                    row['gerencia'],
                    row['reg_hidrog'],
                    row['nome_municipio'],
                    row['ini_monito'].date() if pd.notnull(row['ini_monito']) else None,
                    row['ano_constr'],
                    row['rio_barrad'],
                    row['ac_jusante'],
                    row['id_ac_jus'],
                    row['area_ha'],
                    row['capacid_m3'],
                    row['cot_vert_m'],
                    row['lg_vert_m'],
                    row['cot_td_m'],
                    row['tipo_verte'],
                    row['x'],
                    row['y'],
                    row['longitude'],
                    row['latitude'],
                    row['wkt_geometry'],
                    row['area_calculada']
                ))
            except Exception as e:
                print(f"Erro ao inserir registro {row.get('id_sagreh','?')} - {row.get('nome','?')}: {str(e)}")
                continue
        
        conn.commit()
        print(f"Importação concluída! {len(df)} registros processados na tabela {table_name}.")
    except Exception as e:
        print(f"Erro durante a importação: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    importar_reservatorios()
