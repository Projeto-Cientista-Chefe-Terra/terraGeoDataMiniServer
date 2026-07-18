"""
Testes de /geojson_assentamentos e /assentamentos_municipios.

NOTA: /geojson_assentamentos atualmente retorna 500 para QUALQUER requisição
sob DATABASE_TYPE=sqlite (função SQL "ST_AsGeoJSON" inexistente no
SpatiaLite instalado). Ver tests/test_known_issues.py para os testes que
documentam e comprovam esse bug -- aqui só testamos o que de fato funciona
(/assentamentos_municipios, que não depende de geração de geometria, e a
validação de auth de /geojson_assentamentos, que roda antes da query SQL).
"""


def test_geojson_assentamentos_requires_auth(client):
    resp = client.get("/geojson_assentamentos")
    assert resp.status_code == 401


def test_assentamentos_municipios_lists_distinct_sorted(client, auth_headers):
    resp = client.get("/assentamentos_municipios", headers=auth_headers)
    assert resp.status_code == 200
    municipios = resp.json()["municipios"]
    assert municipios == sorted(municipios)
    # Todos os municípios com registro em assentamentos_ceara aparecem aqui,
    # mesmo o que tem geometria "quebrada" (esta rota não olha geometria).
    assert set(municipios) == {"Fortaleza", "Sobral", "MunicipioTextoWKT"}


def test_assentamentos_municipios_requires_auth(client):
    resp = client.get("/assentamentos_municipios")
    assert resp.status_code == 401
