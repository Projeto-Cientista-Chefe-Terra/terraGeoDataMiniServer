"""Testes de /geojson_muni (tabela municipios_ceara / TABLE_GEOM_MUNICIPIOS)."""


def test_geojson_muni_specific_municipio(client, auth_headers):
    resp = client.get("/geojson_muni", params={"municipio": "Fortaleza"}, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == 1
    feature = body["features"][0]
    assert feature["type"] == "Feature"
    assert feature["geometry"]["type"] == "Polygon"
    assert feature["properties"]["nome_municipio"] == "Fortaleza"
    assert body["properties"]["total_municipios"] == 1


def test_geojson_muni_is_case_insensitive(client, auth_headers):
    resp = client.get("/geojson_muni", params={"municipio": "SOBRAL"}, headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


def test_geojson_muni_todos_returns_every_municipio(client, auth_headers):
    resp = client.get("/geojson_muni", params={"municipio": "todos"}, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    # 3 municípios cadastrados em municipios_ceara: Fortaleza, Sobral, Iguatu.
    assert len(body["features"]) == 3
    assert body["properties"]["total_municipios"] == 3


def test_geojson_muni_todos_is_case_insensitive(client, auth_headers):
    resp = client.get("/geojson_muni", params={"municipio": "ToDoS"}, headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 3


def test_geojson_muni_not_found_returns_404(client, auth_headers):
    resp = client.get("/geojson_muni", params={"municipio": "MunicipioQueNaoExiste"}, headers=auth_headers)
    assert resp.status_code == 404


def test_geojson_muni_missing_param_returns_422(client, auth_headers):
    resp = client.get("/geojson_muni", headers=auth_headers)
    assert resp.status_code == 422


def test_geojson_muni_requires_auth(client):
    resp = client.get("/geojson_muni", params={"municipio": "Fortaleza"})
    assert resp.status_code == 401
