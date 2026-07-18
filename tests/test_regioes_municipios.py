"""Testes de /regioes, /municipios e /municipios_todos."""


def test_listar_regioes_returns_distinct_sorted_regions(client, auth_headers):
    resp = client.get("/regioes", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "regioes" in body
    # Regiões presentes no fixture: Metropolitana e Norte (a de "SemGeom" e
    # "MunicipioTextoWKT" também é Norte).
    assert body["regioes"] == sorted(set(body["regioes"]))
    assert "Regiao Metropolitana" in body["regioes"]
    assert "Regiao Norte" in body["regioes"]


def test_listar_municipios_by_regiao(client, auth_headers):
    resp = client.get("/municipios", params={"regiao": "Regiao Metropolitana"}, headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json() == {"municipios": ["Fortaleza"]}


def test_listar_municipios_is_case_insensitive(client, auth_headers):
    resp = client.get("/municipios", params={"regiao": "regiao metropolitana"}, headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json() == {"municipios": ["Fortaleza"]}


def test_listar_municipios_regiao_not_found_returns_404(client, auth_headers):
    resp = client.get("/municipios", params={"regiao": "Regiao Inexistente"}, headers=auth_headers)
    assert resp.status_code == 404


def test_listar_municipios_missing_query_param_returns_422(client, auth_headers):
    resp = client.get("/municipios", headers=auth_headers)
    assert resp.status_code == 422


def test_listar_municipios_requires_auth(client):
    resp = client.get("/municipios", params={"regiao": "Regiao Norte"})
    assert resp.status_code == 401


def test_municipios_todos_returns_all_distinct_municipios(client, auth_headers):
    resp = client.get("/municipios_todos", headers=auth_headers)
    assert resp.status_code == 200
    municipios = resp.json()["municipios"]
    assert municipios == sorted(municipios)
    for esperado in ("Fortaleza", "Sobral", "SemGeom", "MunicipioTextoWKT"):
        assert esperado in municipios
