#!/usr/bin/env python3

import os
import psycopg2
import pandas as pd
import numpy as np
from unidecode import unidecode
from dotenv import load_dotenv
import math

load_dotenv()

def validar_coordenada(valor):
    """Valida e formata valores de coordenadas"""
    if pd.isna(valor) or valor in (None, np.nan, ''):
        return None
    try:
        valor_float = float(valor)
        if math.isinf(valor_float) or math.isnan(valor_float):
            return None
        return valor_float
    except (ValueError, TypeError):
        return None

def transformar_coordenadas(x, y, epsg_origem=31984, epsg_destino=4326):
    """Transforma coordenadas de um SRC para outro usando PostGIS"""
    x = validar_coordenada(x)
    y = validar_coordenada(y)
    
    if x is None or y is None or x == 0 or y == 0:
        return None, None
    
    db_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'port': os.getenv('POSTGRES_PORT')
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT 
                ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), %s), %s)),
                ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), %s), %s))
        """, (x, y, epsg_origem, epsg_destino, x, y, epsg_origem, epsg_destino))
        
        lon, lat = cursor.fetchone()
        return validar_coordenada(lon), validar_coordenada(lat)
        
    except Exception as e:
        print(f"Erro ao transformar coordenadas: {str(e)}")
        return None, None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def importar_reservatorios():
    db_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'port': os.getenv('POSTGRES_PORT')
    }
    
    table_name = os.getenv('TABLE_DADOS_RESERVATORIOS')
    csv_path = "data/reservatorios-monitorados.csv"
    
    try:
        # Ler CSV com tratamento robusto
        df = pd.read_csv(
            csv_path,
            encoding='utf-8',
            delimiter=',',
            na_values=['', 'NA', 'N/A', 'NaN', 'None', 'inf', '-inf', 'INF', '-INF']
        )
        
        # Converter colunas numéricas
        numeric_cols = ['x', 'y', 'area_ha', 'capacid_m3', 'cot_vert_m', 'lg_vert_m', 'cot_td_m']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
        
        # Converter colunas de texto
        text_cols = ['nome', 'proprietario', 'gerencia', 'reg_hidrog', 'nome_municipio', 
                    'rio_barrad', 'ac_jusante', 'tipo_verte']
        for col in text_cols:
            df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '': None})
        
        # Converter datas
        date_cols = ['ini_monito']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d') if df[col].notna().any() else None
        
        # Transformar coordenadas com validação
        df['longitude'], df['latitude'] = zip(*df.apply(
            lambda row: transformar_coordenadas(row['x'], row['y']), 
            axis=1
        ))
        
        # Criar geometria apenas para coordenadas válidas
        df['wkt_geometry'] = df.apply(
            lambda row: f"POINT({row['longitude']} {row['latitude']})" 
            if validar_coordenada(row['longitude']) and validar_coordenada(row['latitude'])
            else None, 
            axis=1
        )
        
        # Substituir valores inválidos
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        
        # Conectar ao banco de dados
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Criar tabela
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
                cot_td_m DOUBLE PRECISION,
                tipo_verte TEXT,
                x DOUBLE PRECISION,
                y DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                wkt_geometry TEXT,
                geom GEOMETRY(POINT, 4326)
            );
        """)
        
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        
        # Inserir dados com tratamento de geometria inválida
        for _, row in df.iterrows():
            try:
                geom_value = f"ST_GeomFromText('{row['wkt_geometry']}', 4326)" if row['wkt_geometry'] else 'NULL'
                
                cursor.execute(f"""
                INSERT INTO {table_name} (
                    id_sagreh, nome, proprietario, gerencia, reg_hidrog, nome_municipio,
                    ini_monito, ano_constr, rio_barrad, ac_jusante, id_ac_jus,
                    area_ha, capacid_m3, cot_vert_m, lg_vert_m, cot_td_m, tipo_verte,
                    x, y, longitude, latitude, wkt_geometry, geom
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    {geom_value}
                );
                """, (
                    row['id_sagreh'],
                    row['nome'],
                    row['proprietario'],
                    row['gerencia'],
                    row['reg_hidrog'],
                    row['nome_municipio'],
                    row['ini_monito'],
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
                    row['wkt_geometry']
                ))
            except Exception as e:
                print(f"Erro ao inserir registro {row['id_sagreh']} - {row['nome']}: {str(e)}")
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