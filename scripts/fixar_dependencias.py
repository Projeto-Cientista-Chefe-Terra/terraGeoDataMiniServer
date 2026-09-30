"""Gera requirements.txt e requirements-dev.txt com as versões instaladas no ambiente atual.

Lê as dependências diretas de requirements.in e requirements-dev.in, percorre as
dependências transitivas pelos metadados dos pacotes instalados e grava cada
uma com a versão exata. Rode depois de atualizar o ambiente e passar nos testes.
"""
import re
from importlib import metadata
from pathlib import Path

from packaging.requirements import Requirement

RAIZ = Path(__file__).resolve().parent.parent


def _normalizar(nome: str) -> str:
    return re.sub(r"[-_.]+", "-", nome).lower()


def _diretas(arquivo: Path) -> list[str]:
    linhas = [linha.split("#")[0].strip() for linha in arquivo.read_text(encoding="utf-8").splitlines()]
    return [linha for linha in linhas if linha and not linha.startswith("-")]


def _fechamento(raizes: list[str]) -> dict[str, str]:
    """Versão instalada de cada pacote e de suas dependências válidas neste ambiente."""
    versoes, pendentes = {}, [Requirement(r) for r in raizes]
    while pendentes:
        requisito = pendentes.pop()
        nome = _normalizar(requisito.name)
        if nome in versoes:
            continue
        distribuicao = metadata.distribution(nome)
        versoes[nome] = distribuicao.version
        extras = {""} | set(requisito.extras)
        for texto in distribuicao.requires or []:
            dependencia = Requirement(texto)
            if dependencia.marker is None or any(dependencia.marker.evaluate({"extra": e}) for e in extras):
                pendentes.append(dependencia)
    return versoes


def _gravar(destino: Path, cabecalho: str, versoes: dict[str, str]) -> None:
    corpo = "\n".join(f"{nome}=={versao}" for nome, versao in sorted(versoes.items()))
    destino.write_text(f"{cabecalho}\n{corpo}\n", encoding="utf-8")


def main() -> None:
    producao = _fechamento(_diretas(RAIZ / "requirements.in"))
    _gravar(RAIZ / "requirements.txt",
            "# Gerado por scripts/fixar_dependencias.py a partir de requirements.in. Não edite à mão.", producao)
    desenvolvimento = {k: v for k, v in _fechamento(_diretas(RAIZ / "requirements-dev.in")).items() if k not in producao}
    _gravar(RAIZ / "requirements-dev.txt",
            "# Gerado por scripts/fixar_dependencias.py a partir de requirements-dev.in. Não edite à mão.\n"
            "# Dependências só para testes e lint (não instalar em produção).\n-r requirements.txt", desenvolvimento)
    print(f"{len(producao)} pacotes de produção e {len(desenvolvimento)} de desenvolvimento fixados.")


if __name__ == "__main__":
    main()
