#!/usr/bin/env python3

import os
import psycopg2
import pandas as pd
from unidecode import unidecode
from dotenv import load_dotenv


load_dotenv()

def padronizar_nome_municipio(nome):
    """
    Padroniza o nome do município para minúsculas, sem acentos e substitui espaços por underscore
    """
    if pd.isna(nome) or not isinstance(nome, str):
        return None
    
    # Remove acentos e caracteres especiais
    nome_sem_acentos = unidecode(nome)
    # Converte para minúsculas
    nome_minusculo = nome_sem_acentos.lower()
    # Substitui espaços por underscore
    nome_padronizado = nome_minusculo.replace(' ', '_')
    return nome_padronizado

def processar_nome_assentamento(nome_completo):
    """
    Divide o nome completo do assentamento em município original e nome do assentamento
    """
    if pd.isna(nome_completo) or not isinstance(nome_completo, str):
        return None, None, None
    
    partes = nome_completo.split('-', 1)
    
    if len(partes) == 2:
        municipio_original = partes[0].strip()
        nome_assentamento = partes[1].strip()
    else:
        municipio_original = nome_completo.strip()
        nome_assentamento = None
    
    # Padroniza o nome do município
    nome_municipio = padronizar_nome_municipio(municipio_original) if municipio_original else None
    
    return municipio_original, nome_assentamento, nome_municipio

def importar_assentamentos():
    # Configurações do banco de dados a partir do .env
    db_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'port': os.getenv('POSTGRES_PORT')
    }
    
    # Nome da tabela a partir do .env
    table_name = os.getenv('TABLE_DADOS_ASSENTAMENTOS')
    
    # Caminho para o arquivo CSV
    csv_path = "data/assentamentos_estaduais_federais_ceara.csv"
    
    try:
        # Ler o arquivo CSV
        df = pd.read_csv(csv_path, encoding='utf-8')
              
        # Conectar ao banco de dados
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Cria a tabela se não existir
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            cd_sipra vARCHAR(10),
            nome_municipio VARCHAR(100),
            nome_municipio_original VARCHAR(100),
            nome_assentamento VARCHAR(255),
            area NUMERIC,
            perimetro NUMERIC,
            forma_obtecao VARCHAR(255),
            tipo_assentamento VARCHAR(10),
            num_familias NUMERIC,
            wkt_geometry TEXT,
            geom GEOMETRY(MULTIPOLYGON, 4326)
        );
        """)
        
        
        
        # Limpar a tabela antes da importação (opcional)
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        
        # Inserir os dados
        for _, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO {table_name} (
                cd_sipra, nome_municipio, nome_municipio_original, nome_assentamento,
                area, perimetro, forma_obtecao, tipo_assentamento, num_familias, wkt_geometry, geom
            ) VALUES (
                %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326)
            );
            """, (
                row['cd_sipra'],
                row['nome_municipio'],
                row['nome_municipio_original'],
                row['nome_assentamento'],
                row['area'],
                row['perimetro'],
                row['forma_obtecao'],
                row['tipo_assentamento'],
                row['num_familias'],
                row['wkt_geometry'],
                row['wkt_geometry']
            ))
        
        # Confirmar as alterações
        conn.commit()
        
        print(f"Importação concluída com sucesso! {len(df)} registros inseridos na tabela {table_name}.")
        
    except Exception as e:
        print(f"Erro durante a importação: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    
    finally:
        # Fechar a conexão
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    importar_assentamentos()