#!/usr/bin/env python3

import os
import psycopg2
import pandas as pd
import numpy as np
from unidecode import unidecode
from ..config import settings # Carregar variáveis de ambiente do arquivo .env


def padronizar_nome_municipio(nome):
    if pd.isna(nome) or not isinstance(nome, str):
        return None
    
    nome_sem_acentos = unidecode(nome)
    nome_minusculo = nome_sem_acentos.lower()
    nome_padronizado = nome_minusculo.replace(' ', '_')
    return nome_padronizado

def processar_nome_assentamento(nome_completo):
    if pd.isna(nome_completo) or not isinstance(nome_completo, str):
        return None, None, None
    
    partes = nome_completo.split('-', 1)
    
    if len(partes) == 2:
        municipio_original = partes[0].strip()
        nome_assentamento = partes[1].strip()
    else:
        municipio_original = nome_completo.strip()
        nome_assentamento = None
    
    nome_municipio = padronizar_nome_municipio(municipio_original) if municipio_original else None
    
    return municipio_original, nome_assentamento, nome_municipio

def importar_assentamentos():
    db_config = {
        'host': settings.POSTGRES_HOST,
        'database': settings.POSTGRES_DB,
        'user': settings.POSTGRES_USER,
        'password': settings.POSTGRES_PASSWORD,
        'port': settings.POSTGRES_PORT
    }
    
    table_name = settings.TABLE_DADOS_ASSENTAMENTOS
    csv_path = "../datasets/assentamentos_ceara.csv"
    
    try:
        # Ler CSV tratando valores especiais
        df = pd.read_csv(
            csv_path,
            encoding='utf-8',
        )
        
        # Colunas que devem ser numéricas
        numeric_cols = ['num_familias', 'area', 'perimetro']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].replace({np.nan: None})
        
        df['cd_sipra'] = df['cd_sipra'].astype(str).replace({'nan': None, 'NaN': None, 'None': None})
        
        # Substituir NaN do pandas por None para NULL no PostgreSQL
        df = df.replace({np.nan: None})
        
        # Conectar ao banco de dados
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Criar tabela com tipos otimizados
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                cd_sipra VARCHAR(10),
                nome_municipio VARCHAR(100),
                nome_municipio_original VARCHAR(100),
                nome_assentamento VARCHAR(255),
                area DOUBLE PRECISION,
                perimetro DOUBLE PRECISION,
                forma_obtecao VARCHAR(255),
                tipo_assentamento VARCHAR(10),
                num_familias INTEGER,  -- Alterado para INTEGER
                wkt_geometry TEXT,
                geom GEOMETRY(MULTIPOLYGON, 4326)
            );
        """)
        
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        
        # Inserir dados com tratamento de valores nulos
        for _, row in df.iterrows():
            # Garantir que valores numéricos sejam inteiros ou floats
            num_familias = int(row['num_familias']) if pd.notnull(row['num_familias']) else None
            
            cursor.execute(f"""
            INSERT INTO {table_name} (
                cd_sipra, nome_municipio, nome_municipio_original, nome_assentamento,
                area, perimetro, forma_obtecao, tipo_assentamento, num_familias, wkt_geometry, geom
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326)
            );
            """, (
                row['cd_sipra'] if pd.notnull(row['cd_sipra']) else None,
                row['nome_municipio'] if pd.notnull(row['nome_municipio']) else None,
                row['nome_municipio_original'] if pd.notnull(row['nome_municipio_original']) else None,
                row['nome_assentamento'] if pd.notnull(row['nome_assentamento']) else None,
                row['area'],
                row['perimetro'],
                row['forma_obtecao'] if pd.notnull(row['forma_obtecao']) else None,
                row['tipo_assentamento'] if pd.notnull(row['tipo_assentamento']) else None,
                num_familias,  # Já tratado
                row['wkt_geometry'] if pd.notnull(row['wkt_geometry']) else None,
                row['wkt_geometry'] if pd.notnull(row['wkt_geometry']) else None
            ))
        
        conn.commit()
        print(f"Importação concluída! {len(df)} registros inseridos na tabela {table_name}.")
        
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
    importar_assentamentos()