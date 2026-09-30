"""Endpoint /versao_dados e cache com prazo das listas de regiões e municípios."""
import pytest
from sqlalchemy import text

from config import settings
from data_service import main


@pytest.fixture
def tabela_cargas():
    with main.get_engine().begin() as conn:
        conn.execute(text(f"CREATE TABLE {settings.TABLE_CARGAS} (tabela TEXT PRIMARY KEY, registros INTEGER, concluida_em TEXT)"))
        conn.execute(text(f"INSERT INTO {settings.TABLE_CARGAS} VALUES ('municipios_ceara', 184, '2026-09-30 10:00:00')"))
        conn.execute(text(f"INSERT INTO {settings.TABLE_CARGAS} VALUES ('malha_fundiaria_ceara', 233366, '2026-09-30 10:05:00')"))
    yield
    with main.get_engine().begin() as conn:
        conn.execute(text(f"DROP TABLE {settings.TABLE_CARGAS}"))


def test_sem_registro_de_carga_a_versao_e_nula(client, auth_headers):
    resp = client.get("/versao_dados", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json() == {"versao": None, "cargas": []}


def test_versao_e_a_carga_mais_recente(client, auth_headers, tabela_cargas):
    dados = client.get("/versao_dados", headers=auth_headers).json()
    assert dados["versao"] == "2026-09-30 10:05:00"
    assert {c["tabela"] for c in dados["cargas"]} == {"municipios_ceara", "malha_fundiaria_ceara"}


def test_versao_exige_token(client):
    assert client.get("/versao_dados").status_code == 401


def test_listas_ficam_em_cache_dentro_da_janela_e_expiram_depois(client, monkeypatch):
    main._fetch_regioes.cache_clear()
    monkeypatch.setattr(main, "_janela_cache", lambda: 1)
    main.fetch_regioes()
    main.fetch_regioes()
    assert main._fetch_regioes.cache_info().hits == 1
    monkeypatch.setattr(main, "_janela_cache", lambda: 2)
    main.fetch_regioes()
    assert main._fetch_regioes.cache_info().misses == 2
    main._fetch_regioes.cache_clear()


def test_emissores_aceitam_lista_separada_por_virgula(monkeypatch):
    from config import Settings
    monkeypatch.setenv("JWT_ISSUERS", "dashboard_fundiario_ceara, terra_ai ,outro")
    assert Settings().JWT_ISSUERS == ["dashboard_fundiario_ceara", "terra_ai", "outro"]


def test_origens_cors_aceitam_asterisco_e_lista(monkeypatch):
    from config import Settings
    monkeypatch.setenv("ALLOWED_ORIGINS", "*")
    assert Settings().ALLOWED_ORIGINS == ["*"]
    monkeypatch.setenv("ALLOWED_ORIGINS", "https://terrace.virtual.ufc.br, http://localhost:8501")
    assert Settings().ALLOWED_ORIGINS == ["https://terrace.virtual.ufc.br", "http://localhost:8501"]
