"""Testes de /geojson e /dados_fundiarios (tabela malha_fundiaria_ceara)."""

# Mesmo conjunto de colunas declarado em COMMON_PROPERTY_COLUMNS
# (data_service/main.py), exceto "data_criacao_lote" que /dados_fundiarios
# corta de propósito via COMMON_PROPERTY_COLUMNS[:-1].
DADOS_FUNDIARIOS_COLUMNS = {
    "numero_lote", "numero_incra", "situacao_juridica", "modulo_fiscal", "area",
    "nome_municipio", "nome_proprietario", "nome_distrito", "numero_titulo",
    "regiao_administrativa", "categoria", "nome_municipio_original", "imovel",
}


class TestGeojson:
    def test_requires_regiao_or_municipio(self, client, auth_headers):
        resp = client.get("/geojson", headers=auth_headers)
        assert resp.status_code == 400

    def test_rejects_both_regiao_and_municipio(self, client, auth_headers):
        resp = client.get(
            "/geojson",
            params={"regiao": "Regiao Norte", "municipio": "Sobral"},
            headers=auth_headers,
        )
        assert resp.status_code == 400

    def test_by_municipio_returns_feature_collection(self, client, auth_headers):
        resp = client.get("/geojson", params={"municipio": "Fortaleza"}, headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["type"] == "FeatureCollection"
        # 2 lotes cadastrados para Fortaleza no fixture.
        assert len(body["features"]) == 2
        props = body["features"][0]["properties"]
        assert props["nome_municipio"] == "Fortaleza"
        assert "geom_json" not in props

    def test_by_municipio_is_case_insensitive(self, client, auth_headers):
        resp = client.get("/geojson", params={"municipio": "fortaleza"}, headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()["features"]) == 2

    def test_by_regiao_returns_feature_collection(self, client, auth_headers):
        resp = client.get(
            "/geojson", params={"regiao": "Regiao Metropolitana"}, headers=auth_headers
        )
        assert resp.status_code == 200
        assert len(resp.json()["features"]) == 2

    def test_by_regiao_not_found_returns_404(self, client, auth_headers):
        resp = client.get("/geojson", params={"regiao": "Regiao Inexistente"}, headers=auth_headers)
        assert resp.status_code == 404

    def test_municipio_without_any_geometry_returns_404(self, client, auth_headers):
        # "SemGeom" existe na tabela mas seu único lote tem geometry=NULL.
        resp = client.get("/geojson", params={"municipio": "SemGeom"}, headers=auth_headers)
        assert resp.status_code == 404

    def test_accepts_tolerance_and_decimals_params(self, client, auth_headers):
        resp = client.get(
            "/geojson",
            params={"municipio": "Fortaleza", "tolerance": 0.01, "decimals": 2},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert len(resp.json()["features"]) == 2

    def test_requires_auth(self, client):
        resp = client.get("/geojson", params={"municipio": "Fortaleza"})
        assert resp.status_code == 401


class TestDadosFundiarios:
    def test_requires_regiao_or_municipio(self, client, auth_headers):
        resp = client.get("/dados_fundiarios", headers=auth_headers)
        assert resp.status_code == 400

    def test_rejects_both_regiao_and_municipio(self, client, auth_headers):
        resp = client.get(
            "/dados_fundiarios",
            params={"regiao": "Regiao Norte", "municipio": "Sobral"},
            headers=auth_headers,
        )
        assert resp.status_code == 400

    def test_by_municipio_returns_rows_without_geometry(self, client, auth_headers):
        resp = client.get("/dados_fundiarios", params={"municipio": "Fortaleza"}, headers=auth_headers)
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 2
        assert set(rows[0].keys()) == DADOS_FUNDIARIOS_COLUMNS
        assert "data_criacao_lote" not in rows[0]
        assert "geometry" not in rows[0]

    def test_by_municipio_includes_row_without_geometry(self, client, auth_headers):
        # /dados_fundiarios não filtra por geometria (diferente de /geojson):
        # "SemGeom" tem lote sem geometry mas deve aparecer aqui.
        resp = client.get("/dados_fundiarios", params={"municipio": "SemGeom"}, headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_not_found_returns_404(self, client, auth_headers):
        resp = client.get(
            "/dados_fundiarios", params={"municipio": "MunicipioQueNaoExiste"}, headers=auth_headers
        )
        assert resp.status_code == 404

    def test_requires_auth(self, client):
        resp = client.get("/dados_fundiarios", params={"municipio": "Fortaleza"})
        assert resp.status_code == 401
