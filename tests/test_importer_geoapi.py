"""Testes do importador da GeoAPI do IDACE, sem acessar a GeoAPI real."""
import csv
import os
import unicodedata
import urllib.parse

import pytest
import requests
import tenacity

os.environ.setdefault("TOKEN_GEOAPI", "token-de-teste")

import importer_from_geoapi as geo  # noqa: E402

URL = geo.GeoAPIClient.BASE_URL


@pytest.fixture
def cliente(monkeypatch):
    # Sem espera entre tentativas, para o teste não demorar.
    monkeypatch.setattr(geo.GeoAPIClient, "fetch_data", geo.GeoAPIClient.fetch_data.retry_with(wait=tenacity.wait_none()))
    return geo.GeoAPIClient(token="token-de-teste")


def _normalizar(nome):
    return unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode().lower().replace(" ", "_")


def test_lista_de_municipios_bate_com_a_base_do_ibge():
    caminho = os.path.join(os.path.dirname(__file__), "..", "datasets", "municipios_ceara.csv")
    if not os.path.exists(caminho):
        pytest.skip("datasets/municipios_ceara.csv não disponível")
    csv.field_size_limit(10**9)
    with open(caminho, encoding="utf-8") as arquivo:
        base = {linha["nm_mun"] for linha in csv.DictReader(arquivo)}
    lista = {_normalizar(urllib.parse.unquote(m)) for m in geo.municipios}
    assert lista == base


def test_url_base_usa_https():
    assert URL.startswith("https://")


def test_sem_token_gera_erro_claro(monkeypatch):
    monkeypatch.setenv("TOKEN_GEOAPI", "")
    monkeypatch.setattr(geo, "obter_token_geoapi", geo.obter_token_geoapi)
    import config
    monkeypatch.setattr(config.settings, "TOKEN_GEOAPI", "")
    with pytest.raises(ValueError, match="TOKEN_GEOAPI"):
        geo.GeoAPIClient()


def test_paginacao_junta_todas_as_paginas(cliente, requests_mock):
    paginas = {"0": [{"id": 1}, {"id": 2}], "1": [{"id": 3}, {"id": 4}], "2": [{"id": 5}]}
    requests_mock.get(URL + "SOBRAL", json=lambda req, ctx: paginas.get(req.qs["pagina"][0], []))
    assert [r["id"] for r in cliente.fetch_all("SOBRAL", tamanho=2)] == [1, 2, 3, 4, 5]


def test_paginacao_para_se_a_api_repetir_a_pagina(cliente, requests_mock):
    requests_mock.get(URL + "SOBRAL", json=[{"id": 1}, {"id": 2}])
    assert len(cliente.fetch_all("SOBRAL", tamanho=2)) == 2


def test_envia_token_bearer(cliente, requests_mock):
    requests_mock.get(URL + "SOBRAL", json=[])
    cliente.fetch_data("SOBRAL")
    assert requests_mock.last_request.headers["Authorization"] == "Bearer token-de-teste"


def test_erro_4xx_nao_e_repetido(cliente, requests_mock):
    requests_mock.get(URL + "SOBRAL", status_code=401)
    with pytest.raises(requests.exceptions.HTTPError):
        cliente.fetch_data("SOBRAL")
    assert requests_mock.call_count == 1


def test_erro_5xx_e_repetido_cinco_vezes(cliente, requests_mock):
    requests_mock.get(URL + "SOBRAL", status_code=502)
    with pytest.raises(requests.exceptions.HTTPError):
        cliente.fetch_data("SOBRAL")
    assert requests_mock.call_count == 5


def test_sem_terminal_usa_monitor_por_log(monkeypatch):
    monkeypatch.setattr(geo.sys.stdout, "isatty", lambda: False)
    assert isinstance(geo.criar_monitor(), geo.MonitorSimples)
