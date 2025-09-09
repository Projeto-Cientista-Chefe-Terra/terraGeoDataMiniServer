#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
from pathlib import Path
from typing import Any, List, Dict

# Campos desejados (pode ajustar à vontade)
FIELDS = [
    "id", "lote_id", "municipio", "proprietario", "imovel",
    "codigo_distrito", "ponto_de_referencia", "codigo_municipio", "centroide",
    "nome_distrito", "dhc", "dhm", "situacao_juridica", "sncr", "titulo", "numero"
]

def read_json_any(path: Path) -> Any:
    """
    Lê JSON em vários estilos:
      - JSON padrão (objeto/lista)
      - NDJSON/JSON Lines (uma linha por JSON) -> retorna lista
    Levanta ValueError se não conseguir interpretar.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        # Tenta NDJSON/JSON Lines
        objs = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    objs.append(json.loads(line))
                except json.JSONDecodeError:
                    # Não é JSON lines válido
                    raise ValueError(f"Arquivo não é JSON válido nem NDJSON: {path.name}")
        if not objs:
            raise ValueError(f"NDJSON vazio: {path.name}")
        return objs

def to_records(data: Any) -> List[Dict]:
    """
    Converte o conteúdo lido para uma lista de dicionários (records).
    Suporta:
      - Lista de objetos
      - Dicionário com 'features' (GeoJSON -> usa .properties)
      - Dicionário com alguma lista de objetos dentro
      - Dicionário simples -> vira lista com um item
    """
    # GeoJSON
    if isinstance(data, dict) and "features" in data and isinstance(data["features"], list):
        return [ (feat.get("properties", {}) if isinstance(feat, dict) else {}) for feat in data["features"] ]

    # Lista já de objetos
    if isinstance(data, list):
        # Garante dicionários
        return [x if isinstance(x, dict) else {"value": x} for x in data]

    # Dicionário: tenta achar uma lista de dicts em alguma chave
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list) and v and all(isinstance(i, dict) for i in v):
                return v
        # Último recurso: um único registro
        return [data]

    # Qualquer outra coisa vira um único registro com a chave "value"
    return [{"value": data}]

def normalize_to_dataframe(records: List[Dict]) -> pd.DataFrame:
    """
    Normaliza registros (expande campos aninhados) e garante as colunas FIELDS.
    """
    # Normaliza aninhamentos (a.k.a. json_normalize é vida)
    df = pd.json_normalize(records, sep="_")

    # Garante colunas desejadas (criando vazias quando ausentes)
    for col in FIELDS:
        if col not in df.columns:
            df[col] = None

    # Mantém somente as desejadas, na ordem desejada
    df = df[FIELDS]
    return df

def json_to_csv(json_path: Path, csv_path: Path) -> None:
    data = read_json_any(json_path)
    records = to_records(data)
    df = normalize_to_dataframe(records)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"[OK] {json_path.name} -> {csv_path.name} ({len(df)} linhas)")

def main() -> None:
    cwd = Path(".").resolve()
    json_files = sorted([p for p in cwd.iterdir() if p.is_file() and p.suffix.lower() == ".json"])

    if not json_files:
        print("Nenhum arquivo .json encontrado no diretório atual. 😿")
        return

    ok, fail = 0, 0
    for jf in json_files:
        try:
            csv_path = jf.with_suffix(".csv")
            json_to_csv(jf, csv_path)
            ok += 1
        except Exception as e:
            print(f"[ERRO] {jf.name}: {e}")
            fail += 1

    print("\nResumo:")
    print(f"  Sucesso: {ok}")
    print(f"  Falhas : {fail}")

if __name__ == "__main__":
    main()
