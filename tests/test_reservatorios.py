"""
Testes de /geojson_reservatorios e /reservatorios_municipios.

NOTA: /geojson_reservatorios atualmente retorna 500 para QUALQUER requisição
sob DATABASE_TYPE=sqlite (função SQL "ST_AsGeoJSON" inexistente no
SpatiaLite instalado, mesma causa raiz do /geojson_assentamentos). Ver
tests/test_known_issues.py para os testes que documentam e comprovam esse
bug -- aqui só testamos o que de fato funciona (/reservatorios_municipios,
que não depende de geração de geometria, e a validação de auth de
/geojson_reservatorios, que roda antes da query SQL).
"""


def test_geojson_reservatorios_requires_auth(client):
    resp = client.get("/geojson_reservatorios")
    assert resp.status_code == 401


def test_reservatorios_municipios_lists_distinct_sorted(client, auth_headers):
    resp = client.get("/reservatorios_municipios", headers=auth_headers)
    assert resp.status_code == 200
    municipios = resp.json()["municipios"]
    assert municipios == sorted(municipios)
    assert set(municipios) == {"Fortaleza", "Sobral"}


def test_reservatorios_municipios_requires_auth(client):
    resp = client.get("/reservatorios_municipios")
    assert resp.status_code == 401
