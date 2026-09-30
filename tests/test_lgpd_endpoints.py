"""Nomes de proprietários nas respostas: protegidos conforme a LGPD, com pseudônimo para agrupar."""
import re

from data_service.privacidade import TEXTO_NOME_PROTEGIDO


def test_dados_fundiarios_nao_trazem_nome_de_pessoa_fisica(client, auth_headers):
    resp = client.get("/dados_fundiarios", params={"municipio": "Fortaleza"}, headers=auth_headers)
    assert resp.status_code == 200
    registros = resp.json()
    assert {r["nome_proprietario"] for r in registros} == {TEXTO_NOME_PROTEGIDO}
    assert "Joao da Silva" not in resp.text and "Maria Souza" not in resp.text


def test_dados_fundiarios_trazem_pseudonimo_distinto_por_proprietario(client, auth_headers):
    registros = client.get("/dados_fundiarios", params={"municipio": "Fortaleza"}, headers=auth_headers).json()
    ids = [r["id_proprietario"] for r in registros]
    assert all(re.fullmatch(r"[0-9a-f]{32}", i) for i in ids)
    assert len(set(ids)) == len(ids) == 2


def test_geojson_nao_traz_nome_de_pessoa_fisica(client, auth_headers):
    resp = client.get("/geojson", params={"municipio": "Fortaleza"}, headers=auth_headers)
    assert resp.status_code == 200
    nomes = {f["properties"]["nome_proprietario"] for f in resp.json()["features"]}
    assert nomes == {TEXTO_NOME_PROTEGIDO}
    assert "Joao da Silva" not in resp.text
