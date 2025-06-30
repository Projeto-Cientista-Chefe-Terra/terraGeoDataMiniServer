# data_service/utils.py

import math
import json
from typing import Any, Mapping

def safe_value(val: Any) -> Any:
    """Substitui NaN por None para não gerar JSON inválido."""
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def row_to_feature(mapping: Mapping[str, Any]) -> dict:
    """
    Converte um RowMapping (dict) em um Feature GeoJSON:
    - mapping['geom_json'] é esperado ser uma string JSON.
    """
    # 1) Extrai e desserializa a geometria
    geom_json_str = mapping['geom_json']
    geometry = json.loads(geom_json_str)  # aceita apenas str/bytes/bytearray :contentReference[oaicite:4]{index=4}

    # 2) Monta propriedades excluindo a geometria
    props = {
        key: safe_value(value)
        for key, value in mapping.items()
        if key != 'geom_json'
    }

    return {
        "type": "Feature",
        "geometry": geometry,
        "properties": props
    }

