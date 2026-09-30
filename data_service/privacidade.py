# data_service/privacidade.py
"""Tratamento de dados pessoais (LGPD) nas respostas da API.

A LGPD (Lei 13.709/2018) protege dados de pessoas naturais. A API não entrega
o nome de proprietário pessoa física: ele sai trocado por um texto fixo. O
nome de pessoa jurídica ou de ente público não é dado pessoal e sai como está.

A distinção é conservadora e segue a mesma regra do dashboard
(``dashboard_fundiario_ceara/modules/privacidade.py``): só é pessoa jurídica o
nome que termina em LTDA ou S/A ou que traz um termo institucional inequívoco.
Espólio e empresário individual (ME, MEI, EIRELI), que costumam carregar o nome
da pessoa, ficam ocultos.

Para o cálculo de concentração fundiária (Gini), o dashboard precisa agrupar
os imóveis de um mesmo proprietário. Para isso a API entrega
``id_proprietario``: um HMAC-SHA256 do nome normalizado. É um pseudônimo
estável, igual em todos os workers e em todas as regiões, que não permite
recuperar o nome sem a chave.
"""

import hashlib
import hmac
import re
import unicodedata
from functools import lru_cache
from typing import Any, Optional

from config import settings

TEXTO_NOME_PROTEGIDO = "Pessoa física (protegido pela LGPD)"

_TERMOS_INSTITUCIONAIS = [
    "ASSOCIACAO", "ASSOC", "COOPERATIVA", "SINDICATO", "FUNDACAO", "INSTITUTO",
    "EMPRESA", "COMPANHIA", "CIA", "AGROPECUARIA", "AGROINDUSTRIA", "AGROINDUSTRIAL",
    "INDUSTRIA", "INDUSTRIAL", "COMERCIO", "COMERCIAL", "MINERACAO", "MINERADORA",
    "CONSTRUTORA", "INCORPORADORA", "EMPREENDIMENTOS", "PARTICIPACOES", "HOLDING",
    "CONDOMINIO", "CONSORCIO", "SOCIEDADE", "IGREJA", "PAROQUIA", "DIOCESE",
    "ARQUIDIOCESE", "MITRA", "CONGREGACAO", "PREFEITURA", "MUNICIPIO", "CAMARA MUNICIPAL",
    "ESTADO DO CEARA", "GOVERNO", "UNIAO FEDERAL", "SECRETARIA", "INCRA", "IDACE",
    "DNOCS", "CAGECE", "COGERH", "EMBRAPA", "UNIVERSIDADE", "ESCOLA", "COLEGIO",
    "HOSPITAL", "BANCO", "EOLICA", "ENERGIA",
]
_REGEX_INSTITUCIONAL = re.compile(r"\b(?:" + "|".join(_TERMOS_INSTITUCIONAIS) + r")\b")
# "SA" sem pontuação fica de fora (Sá é sobrenome comum), assim como ME, MEI,
# EPP e EIRELI (o empresário individual costuma usar o próprio nome).
_REGEX_FORMA_JURIDICA = re.compile(r"(?:\bLTDA\.?|\bS\s?/\s?A\.?|\bS\.A\.?)\s*$")


def _vazio(valor: Any) -> bool:
    if valor is None:
        return True
    if isinstance(valor, float) and valor != valor:
        return True
    return isinstance(valor, str) and valor.strip().lower() in ("", "nan", "none", "null")


def _normalizar_para_regra(texto: str) -> str:
    sem_acento = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", sem_acento.upper()).strip()


def normalizar_nome(nome: str) -> str:
    """Forma usada para agrupar proprietários: sem acento, minúsculas, sem espaços nas pontas.

    É a mesma normalização que o dashboard aplicava ao nome antes desta mudança.
    """
    return unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii").lower().strip()


def eh_pessoa_juridica(nome: Optional[str]) -> bool:
    """Indica se o nome identifica com segurança uma pessoa jurídica ou ente público."""
    if _vazio(nome):
        return False
    normalizado = _normalizar_para_regra(str(nome))
    return bool(_REGEX_FORMA_JURIDICA.search(normalizado) or _REGEX_INSTITUCIONAL.search(normalizado))


def nome_proprietario_publico(nome: Optional[str]) -> Optional[str]:
    """Nome que pode sair na API: o nome da pessoa jurídica, o texto fixo ou None se ausente."""
    if _vazio(nome):
        return None
    if eh_pessoa_juridica(nome):
        return str(nome).strip()
    return TEXTO_NOME_PROTEGIDO


@lru_cache(maxsize=1)
def _chave_pseudonimo() -> bytes:
    """Chave do HMAC. Usa PSEUDONIMIZACAO_SECRET ou, se vazia, uma derivação do JWT_SECRET.

    A derivação garante o mesmo pseudônimo em todos os workers do gunicorn sem
    exigir uma variável nova na stack.
    """
    base = settings.PSEUDONIMIZACAO_SECRET or settings.JWT_SECRET
    return hmac.new(base.encode("utf-8"), b"terra-pseudonimo-proprietario", hashlib.sha256).digest()


def pseudonimo_proprietario(nome: Optional[str]) -> Optional[str]:
    """Identificador estável do proprietário, sem revelar o nome. None se o nome estiver ausente."""
    if _vazio(nome):
        return None
    normalizado = normalizar_nome(str(nome))
    if not normalizado:
        return None
    return hmac.new(_chave_pseudonimo(), normalizado.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
