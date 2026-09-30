"""Testes de data_service/privacidade.py: regra da LGPD e pseudônimo do proprietário."""
import re

import pytest

from data_service import privacidade as pv


@pytest.mark.parametrize("nome", [
    "José da Silva", "MARIA DAS GRAÇAS PEREIRA", "JOAO DE SA", "Espólio de Fulano de Tal",
    "JOAO DA SILVA ME", "MARIA SOUZA MEI", "PEDRO ALVES EIRELI", "Lucia Ciarlini",
])
def test_pessoa_fisica_fica_protegida(nome):
    assert not pv.eh_pessoa_juridica(nome)
    assert pv.nome_proprietario_publico(nome) == pv.TEXTO_NOME_PROTEGIDO


@pytest.mark.parametrize("nome", [
    "Agropecuária Boa Vista Ltda", "EMPRESA X S/A", "Energia dos Ventos S.A.",
    "ASSOCIAÇÃO DOS PEQUENOS PRODUTORES", "Prefeitura Municipal de Canindé", "ESTADO DO CEARÁ",
    "DIOCESE DE SOBRAL", "DEPARTAMENTO NACIONAL DE OBRAS CONTRA AS SECAS- DNOCS",
])
def test_pessoa_juridica_e_ente_publico_saem_como_estao(nome):
    assert pv.nome_proprietario_publico(nome) == nome


@pytest.mark.parametrize("valor", [None, "", "   ", "nan", float("nan")])
def test_nome_ausente_vira_none(valor):
    assert pv.nome_proprietario_publico(valor) is None
    assert pv.pseudonimo_proprietario(valor) is None


def test_pseudonimo_e_estavel_e_nao_contem_o_nome():
    primeiro = pv.pseudonimo_proprietario("José da Silva")
    assert primeiro == pv.pseudonimo_proprietario("José da Silva")
    assert re.fullmatch(r"[0-9a-f]{32}", primeiro)
    assert "jose" not in primeiro


def test_pseudonimo_agrupa_variacoes_de_acento_e_caixa():
    assert pv.pseudonimo_proprietario("JOSÉ DA SILVA ") == pv.pseudonimo_proprietario("jose da silva")


def test_pseudonimos_de_pessoas_diferentes_diferem():
    assert pv.pseudonimo_proprietario("Ana") != pv.pseudonimo_proprietario("Beatriz")


def test_pseudonimo_depende_da_chave(monkeypatch):
    antes = pv.pseudonimo_proprietario("Ana")
    monkeypatch.setattr(pv.settings, "PSEUDONIMIZACAO_SECRET", "outra-chave-com-mais-de-32-bytes-000000")
    pv._chave_pseudonimo.cache_clear()
    try:
        assert pv.pseudonimo_proprietario("Ana") != antes
    finally:
        pv._chave_pseudonimo.cache_clear()
