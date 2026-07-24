"""
Testes de regressão para a correção de segurança que removeu o default
público de JWT_SECRET em config.py, corrigiu env.template (chaves faltando
e TABLE_DADOS_ASSENTAMENTOS) e adicionou entradas a .dockerignore.

Notas de isolamento:

- O projeto tem um `.env` real em terraGeoDataMiniServer/.env (fora do
  controle de versão) com JWT_SECRET já definido. Como
  `config.Settings.model_config` usa `env_file=".env"` (caminho relativo ao
  cwd do processo pytest, que é terraGeoDataMiniServer/), simplesmente
  remover JWT_SECRET de os.environ não é suficiente para provar que o
  campo é obrigatório: o pydantic-settings ainda leria o valor do .env em
  disco. Por isso o teste de ausência de JWT_SECRET instancia
  `Settings(_env_file=None)` para desligar a leitura do .env nesse caso
  específico, isolando exatamente o efeito da variável de ambiente.

- Os testes de arquivo (env.template, .dockerignore, config.py) resolvem
  caminhos relativos a este arquivo de teste (Path(__file__)...), não ao
  cwd, para funcionar independente de onde o pytest for invocado.
"""
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from config import Settings

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_settings_requires_jwt_secret_without_env_file(monkeypatch):
    """
    Sem JWT_SECRET no ambiente (e sem cair de volta no .env em disco), a
    instanciação de Settings deve falhar com ValidationError apontando
    para o campo JWT_SECRET. Isso é a regressão central da correção de
    segurança: antes, JWT_SECRET tinha um default público
    ("eh_segredo_voce_nao_deve_ler_isto") e a ausência da variável não
    quebrava nada silenciosamente.
    """
    monkeypatch.delenv("JWT_SECRET", raising=False)
    # Mantém as outras variáveis obrigatórias definidas, para isolar que é
    # especificamente a ausência de JWT_SECRET que causa o erro.
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("DATABASE_TYPE", "sqlite")

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    errors = exc_info.value.errors()
    assert any(error["loc"] == ("JWT_SECRET",) for error in errors), (
        f"Esperava um erro de validação para o campo JWT_SECRET, "
        f"mas os erros foram: {errors}"
    )


def test_settings_succeeds_when_jwt_secret_is_set(monkeypatch):
    """
    Contraprova do teste acima: com JWT_SECRET (e as demais variáveis
    obrigatórias) definido, Settings() instancia normalmente.
    """
    monkeypatch.setenv("JWT_SECRET", "some-test-secret")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("DATABASE_TYPE", "sqlite")

    settings = Settings(_env_file=None)
    assert settings.JWT_SECRET == "some-test-secret"


def test_config_py_does_not_contain_hardcoded_secret_default():
    """
    Regressão simples de leitura de arquivo: o antigo default público de
    JWT_SECRET não deve mais aparecer em config.py.
    """
    config_source = (REPO_ROOT / "config.py").read_text(encoding="utf-8")
    assert "eh_segredo_voce_nao_deve_ler_isto" not in config_source


def test_dockerignore_covers_env_file():
    """
    .dockerignore deve conter uma entrada que impede o .env de ser
    copiado para dentro da imagem Docker.
    """
    dockerignore_content = (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8")
    lines = [line.strip() for line in dockerignore_content.splitlines()]
    assert ".env" in lines, (
        f".dockerignore deveria conter a linha exata '.env', "
        f"linhas encontradas: {lines}"
    )


@pytest.mark.parametrize(
    "expected_key",
    [
        "JWT_SECRET",
        "JWT_ALGORITHM",
        "JWT_EXPIRE_MINUTES",
        "TOKEN_GEOAPI",
        "TABLE_DADOS_RESERVATORIOS",
        "TABLE_TEMPORARY",
        "TABLE_RA_MUNICIPIOS_MF_CE",
        "TGDMSERVER_HOST",
        "TGDMSERVER_PORT",
    ],
)
def test_env_template_contains_previously_missing_keys(expected_key):
    """
    Todas as chaves que antes faltavam em env.template agora devem
    aparecer no arquivo (uma linha começando com "CHAVE=").
    """
    env_template_content = (REPO_ROOT / "env.template").read_text(encoding="utf-8")
    lines = env_template_content.splitlines()
    assert any(line.startswith(f"{expected_key}=") for line in lines), (
        f"Esperava encontrar uma linha '{expected_key}=...' em env.template"
    )


def test_env_template_table_dados_assentamentos_is_correct():
    """
    TABLE_DADOS_ASSENTAMENTOS foi corrigido em env.template; garante que a
    chave existe com um valor não vazio (regressão do valor incorreto
    anterior).
    """
    env_template_content = (REPO_ROOT / "env.template").read_text(encoding="utf-8")
    lines = env_template_content.splitlines()
    matching = [line for line in lines if line.startswith("TABLE_DADOS_ASSENTAMENTOS=")]
    assert matching, "TABLE_DADOS_ASSENTAMENTOS não encontrado em env.template"
    key, _, value = matching[0].partition("=")
    assert value.strip().strip('"') != "", (
        "TABLE_DADOS_ASSENTAMENTOS está presente mas sem valor em env.template"
    )
