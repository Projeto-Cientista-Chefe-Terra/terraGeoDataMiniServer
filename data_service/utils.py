import math
import json

def safe_value(val):
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def row_to_feature(row, colnames):
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
