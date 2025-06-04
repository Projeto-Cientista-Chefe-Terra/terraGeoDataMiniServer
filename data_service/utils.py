# data_service/utils.py

import math
import json

def safe_value(val):
    """
    Converte float NaN em None (para JSON) ou devolve o próprio valor.
    """
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def row_to_feature(row, colnames):
    """
    Converte uma tupla de linha (row[0] = geom_json_str, row[1:] = props) e
    uma lista colnames = ["geom_json", "col1", "col2", ...] em um dict GeoJSON Feature:
      {
        "type": "Feature",
        "geometry": <parsed do geom_json_str>,
        "properties": { "col1": val1, "col2": val2, ... }
      }
    """
    geom_json_str = row[0]
    geometry = json.loads(geom_json_str)

    props = {}
    for idx, col in enumerate(colnames[1:], start=1):
        props[col] = safe_value(row[idx])

    return {
        "type": "Feature",
        "geometry": geometry,
        "properties": props
    }
