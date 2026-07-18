"""
Fixtures compartilhadas para a suíte de testes do terraGeoDataMiniServer.

Ordem de inicialização importa MUITO neste arquivo:

1. `config.settings = Settings()` é instanciado uma única vez, no momento em
   que `config.py` é importado pela primeira vez (module-level). Qualquer
   variável de ambiente que os testes precisem sobrepor (DATABASE_TYPE,
   SQLITE_PATH, JWT_SECRET, ...) tem que estar em `os.environ` ANTES dessa
   primeira importação — daí elas serem setadas logo no topo deste módulo,
   antes de `import data_service.main`. O pytest garante que conftest.py é
   carregado antes de qualquer módulo de teste, então isso é suficiente
   desde que nenhum teste importe `config`/`data_service.main` diretamente
   sem passar pelas fixtures daqui.

2. O Python do .venv (instalado via asdf/python-build) foi compilado sem
   suporte a "loadable extensions" no módulo `sqlite3` da stdlib
   (`sqlite3.Connection` não tem `enable_load_extension`). Isso impede
   carregar a extensão mod_spatialite, que `data_service/db.py` carrega em
   todo `connect()` quando DATABASE_TYPE=sqlite. Para os testes, resolvemos
   isso substituindo `sys.modules["sqlite3"]` pelo pacote pysqlite3-binary
   (que embute um SQLite com essa opção habilitada) ANTES de qualquer código
   importar sqlalchemy/sqlite3 — o dialeto sqlite do SQLAlchemy faz
   `import sqlite3` de forma lazy, então isso precisa acontecer cedo.
   Ver nota equivalente em requirements-dev.txt e o resumo final da tarefa:
   isso pode ser um problema real para quem rodar DATABASE_TYPE=sqlite fora
   dos testes com esse mesmo interpretador.
"""
import os
import sys
import shutil
import tempfile

# ---------------------------------------------------------------------------
# 1) sqlite3 -> pysqlite3 (suporte a extensões carregáveis, ver docstring)
#
# SQLAlchemy's sqlite dialect faz `from sqlite3 import dbapi2 as sqlite`, ou
# seja, precisa tanto de sys.modules["sqlite3"] quanto de
# sys.modules["sqlite3.dbapi2"] (import de submódulo) apontando para o
# pysqlite3 correspondente.
# ---------------------------------------------------------------------------
import pysqlite3
import pysqlite3.dbapi2 as _pysqlite3_dbapi2

sys.modules["sqlite3"] = pysqlite3
sys.modules["sqlite3.dbapi2"] = _pysqlite3_dbapi2

# ---------------------------------------------------------------------------
# 2) Configuração de ambiente de teste (antes de importar `config`)
# ---------------------------------------------------------------------------
TEST_JWT_SECRET = "pytest-only-secret-do-not-use-in-prod"
TEST_JWT_ALGORITHM = "HS256"

_TMP_DIR = tempfile.mkdtemp(prefix="tgdm_test_")
TEST_SQLITE_PATH = os.path.join(_TMP_DIR, "test_terra_data.sqlite")

os.environ.update(
    {
        "DATABASE_TYPE": "sqlite",
        "SQLITE_PATH": TEST_SQLITE_PATH,
        # Obrigatórios pelo modelo Settings, mas nunca usados (DATABASE_TYPE=sqlite).
        "POSTGRES_USER": "unused",
        "POSTGRES_PASSWORD": "unused",
        "POSTGRES_HOST": "unused",
        "POSTGRES_DB": "unused",
        "JWT_SECRET": TEST_JWT_SECRET,
        "JWT_ALGORITHM": TEST_JWT_ALGORITHM,
        "JWT_EXPIRE_MINUTES": "30",
        "TABLE_GEOM_MUNICIPIOS": "municipios_ceara",
        "TABLE_DADOS_FUNDIARIOS": "malha_fundiaria_ceara",
        "TABLE_DADOS_ASSENTAMENTOS": "assentamentos_ceara",
        "TABLE_DADOS_RESERVATORIOS": "reservatorios_ceara",
        "TABLE_RA_MUNICIPIOS_MF_CE": "regioes_administrativas_municipios_malha_fundiaria_ceara",
    }
)

import jwt  # noqa: E402  (import após setup de ambiente, de propósito)
import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

SPATIALITE_EXTENSION = "mod_spatialite"


def _connect_raw():
    """Conexão crua (fora do SQLAlchemy) usada só para montar o fixture DB."""
    conn = _pysqlite3_dbapi2.connect(TEST_SQLITE_PATH)
    conn.enable_load_extension(True)
    conn.load_extension(SPATIALITE_EXTENSION)
    return conn


def _wkt_square(seed: int) -> str:
    """Quadrado 1x1 determinístico, só para ter uma geometria válida."""
    x, y = float(seed), float(seed)
    return f"POLYGON(({x} {y},{x} {y + 1},{x + 1} {y + 1},{x + 1} {y},{x} {y}))"


# ==========================================================================
# Dados sintéticos usados pelos testes (mantidos aqui para fácil referência
# cruzada entre os módulos de teste).
# ==========================================================================
FUNDIARIOS_ROWS = [
    dict(
        lote_id=1, nome_municipio="Fortaleza", nome_municipio_original="FORTALEZA",
        nome_proprietario="Joao da Silva", imovel="Sitio Boa Vista", nome_distrito="Sede",
        data_criacao_lote="2020-01-10", situacao_juridica="Regularizado",
        numero_incra="INCRA-001", numero_titulo="TIT-001", numero_lote="L-01",
        regiao_administrativa="Regiao Metropolitana", modulo_fiscal=2, area=15.5,
        categoria="Lote", wkt=_wkt_square(0),
    ),
    dict(
        lote_id=2, nome_municipio="Fortaleza", nome_municipio_original="FORTALEZA",
        nome_proprietario="Maria Souza", imovel="Sitio Alegre", nome_distrito="Sede",
        data_criacao_lote="2021-03-22", situacao_juridica="Em analise",
        numero_incra="INCRA-002", numero_titulo=None, numero_lote="L-02",
        regiao_administrativa="Regiao Metropolitana", modulo_fiscal=3, area=8.2,
        categoria="Lote", wkt=_wkt_square(1),
    ),
    dict(
        lote_id=3, nome_municipio="Sobral", nome_municipio_original="SOBRAL",
        nome_proprietario="Pedro Lima", imovel="Fazenda Sao Jose", nome_distrito="Centro",
        data_criacao_lote="2019-07-05", situacao_juridica="Regularizado",
        numero_incra="INCRA-003", numero_titulo="TIT-003", numero_lote="L-03",
        regiao_administrativa="Regiao Norte", modulo_fiscal=4, area=42.0,
        categoria="Lote", wkt=_wkt_square(2),
    ),
    # Município só com registro sem geometria: usado para exercitar o 404
    # de "nenhuma geometria encontrada" em /geojson.
    dict(
        lote_id=4, nome_municipio="SemGeom", nome_municipio_original="SEMGEOM",
        nome_proprietario="Ana Costa", imovel="Chacara Sem Nome", nome_distrito="Zona Rural",
        data_criacao_lote="2022-02-02", situacao_juridica="Pendente",
        numero_incra=None, numero_titulo=None, numero_lote="L-04",
        regiao_administrativa="Regiao Norte", modulo_fiscal=1, area=3.1,
        categoria="Lote", wkt=None,
    ),
    # Município cuja coluna "geometry" guarda WKT como TEXTO puro (não um
    # blob SpatiaLite), reproduzindo o que a tabela equivalente do Postgres
    # de fato armazena (`geometry TEXT` em importer_all.py). Ver
    # tests/test_known_issues.py.
    dict(
        lote_id=5, nome_municipio="MunicipioTextoWKT", nome_municipio_original="MUNICIPIOTEXTOWKT",
        nome_proprietario="Carlos Reis", imovel="Lote Teste", nome_distrito="Sede",
        data_criacao_lote="2023-05-15", situacao_juridica="Regularizado",
        numero_incra="INCRA-005", numero_titulo="TIT-005", numero_lote="L-05",
        regiao_administrativa="Regiao Norte", modulo_fiscal=1, area=1.0,
        categoria="Lote", wkt=None, wkt_as_text=_wkt_square(5),
    ),
]

MUNICIPIOS_GEOM_ROWS = [
    dict(nm_mun="Fortaleza", wkt=_wkt_square(0)),
    dict(nm_mun="Sobral", wkt=_wkt_square(2)),
    dict(nm_mun="Iguatu", wkt=_wkt_square(3)),
]

ASSENTAMENTOS_ROWS = [
    dict(
        cd_sipra="SP0001", nome_municipio="Fortaleza", nome_municipio_original="FORTALEZA",
        nome_assentamento="Assentamento Esperanca", area=120.5, perimetro=45.0,
        tipo_assentamento="PA", forma_obtecao="Desapropriacao", num_familias=30,
        wkt=_wkt_square(10),
    ),
    dict(
        cd_sipra="SP0002", nome_municipio="Sobral", nome_municipio_original="SOBRAL",
        nome_assentamento="Assentamento Nova Vida", area=80.0, perimetro=30.0,
        tipo_assentamento="PA", forma_obtecao="Compra", num_familias=15,
        wkt=_wkt_square(11),
    ),
    # Reproduz o schema real do Postgres (wkt_geometry como TEXTO puro) para
    # o teste de regressão em tests/test_known_issues.py.
    dict(
        cd_sipra="SP0003", nome_municipio="MunicipioTextoWKT", nome_municipio_original="MUNICIPIOTEXTOWKT",
        nome_assentamento="Assentamento Texto", area=10.0, perimetro=5.0,
        tipo_assentamento="PA", forma_obtecao="Doacao", num_familias=5,
        wkt=None, wkt_as_text=_wkt_square(12),
    ),
]

RESERVATORIOS_ROWS = [
    dict(
        id_sagreh=1, nome="Acude Central", proprietario="DNOCS", gerencia="COGERH",
        reg_hidrog="Bacia Metropolitana", nome_municipio="Fortaleza",
        nome_municipio_original="FORTALEZA", ini_monito="1990-01-01", ano_constr=1985,
        o_barrad="Terra", ac_jusante=1, id_ac_jus=1.0, area_ha=250.0, capacid_m3=1_500_000.0,
        cot_vert_m=45.2, lg_vert_m=12.0, cot_td_m="44.0", tipo_verte=1.0, ri="RI-001",
        wkt=_wkt_square(20),
    ),
    dict(
        id_sagreh=2, nome="Acude Sobral", proprietario="Estado", gerencia="COGERH",
        reg_hidrog="Bacia Acarau", nome_municipio="Sobral",
        nome_municipio_original="SOBRAL", ini_monito="1995-06-01", ano_constr=1990,
        o_barrad="Concreto", ac_jusante=0, id_ac_jus=0.0, area_ha=90.0, capacid_m3=300_000.0,
        cot_vert_m=30.1, lg_vert_m=8.0, cot_td_m="29.0", tipo_verte=2.0, ri="RI-002",
        wkt=_wkt_square(21),
    ),
]


def _build_database() -> None:
    conn = _connect_raw()
    try:
        cur = conn.cursor()

        # ---- malha_fundiaria_ceara -----------------------------------
        cur.execute(
            """
            CREATE TABLE malha_fundiaria_ceara (
                id INTEGER PRIMARY KEY,
                lote_id INTEGER,
                nome_municipio TEXT,
                nome_municipio_original TEXT,
                nome_proprietario TEXT,
                imovel TEXT,
                nome_distrito TEXT,
                data_criacao_lote TEXT,
                situacao_juridica TEXT,
                numero_incra TEXT,
                numero_titulo TEXT,
                numero_lote TEXT,
                regiao_administrativa TEXT,
                modulo_fiscal INTEGER,
                area REAL,
                categoria TEXT,
                geometry BLOB
            )
            """
        )
        for r in FUNDIARIOS_ROWS:
            base_cols = (
                "lote_id, nome_municipio, nome_municipio_original, nome_proprietario, imovel,"
                " nome_distrito, data_criacao_lote, situacao_juridica, numero_incra, numero_titulo,"
                " numero_lote, regiao_administrativa, modulo_fiscal, area, categoria"
            )
            base_vals = [
                r["lote_id"], r["nome_municipio"], r["nome_municipio_original"], r["nome_proprietario"],
                r["imovel"], r["nome_distrito"], r["data_criacao_lote"], r["situacao_juridica"],
                r["numero_incra"], r["numero_titulo"], r["numero_lote"], r["regiao_administrativa"],
                r["modulo_fiscal"], r["area"], r["categoria"],
            ]
            if r.get("wkt"):
                cur.execute(
                    f"INSERT INTO malha_fundiaria_ceara ({base_cols}, geometry) "
                    f"VALUES ({','.join('?' * len(base_vals))}, GeomFromText(?, 4326))",
                    base_vals + [r["wkt"]],
                )
            elif r.get("wkt_as_text"):
                # Geometria guardada como TEXTO cru (não-blob), de propósito:
                # ver tests/test_known_issues.py.
                cur.execute(
                    f"INSERT INTO malha_fundiaria_ceara ({base_cols}, geometry) "
                    f"VALUES ({','.join('?' * len(base_vals))}, ?)",
                    base_vals + [r["wkt_as_text"]],
                )
            else:
                cur.execute(
                    f"INSERT INTO malha_fundiaria_ceara ({base_cols}, geometry) "
                    f"VALUES ({','.join('?' * len(base_vals))}, NULL)",
                    base_vals,
                )

        # ---- municipios_ceara ------------------------------------------
        cur.execute(
            """
            CREATE TABLE municipios_ceara (
                id INTEGER PRIMARY KEY,
                nm_mun TEXT,
                geometry BLOB
            )
            """
        )
        for r in MUNICIPIOS_GEOM_ROWS:
            cur.execute(
                "INSERT INTO municipios_ceara (nm_mun, geometry) VALUES (?, GeomFromText(?, 4326))",
                [r["nm_mun"], r["wkt"]],
            )

        # ---- assentamentos_ceara ----------------------------------------
        cur.execute(
            """
            CREATE TABLE assentamentos_ceara (
                id INTEGER PRIMARY KEY,
                cd_sipra TEXT,
                nome_municipio TEXT,
                nome_municipio_original TEXT,
                nome_assentamento TEXT,
                area REAL,
                perimetro REAL,
                tipo_assentamento TEXT,
                forma_obtecao TEXT,
                num_familias INTEGER,
                wkt_geometry BLOB
            )
            """
        )
        base_cols = (
            "cd_sipra, nome_municipio, nome_municipio_original, nome_assentamento, area,"
            " perimetro, tipo_assentamento, forma_obtecao, num_familias"
        )
        for r in ASSENTAMENTOS_ROWS:
            base_vals = [
                r["cd_sipra"], r["nome_municipio"], r["nome_municipio_original"], r["nome_assentamento"],
                r["area"], r["perimetro"], r["tipo_assentamento"], r["forma_obtecao"], r["num_familias"],
            ]
            if r.get("wkt"):
                cur.execute(
                    f"INSERT INTO assentamentos_ceara ({base_cols}, wkt_geometry) "
                    f"VALUES ({','.join('?' * len(base_vals))}, GeomFromText(?, 4326))",
                    base_vals + [r["wkt"]],
                )
            else:
                # wkt_as_text: coluna recebe uma string WKT crua (mesmo shape
                # do Postgres, onde wkt_geometry é TEXT) -- ver test_known_issues.
                cur.execute(
                    f"INSERT INTO assentamentos_ceara ({base_cols}, wkt_geometry) "
                    f"VALUES ({','.join('?' * len(base_vals))}, ?)",
                    base_vals + [r["wkt_as_text"]],
                )

        # ---- reservatorios_ceara ------------------------------------------
        cur.execute(
            """
            CREATE TABLE reservatorios_ceara (
                id INTEGER PRIMARY KEY,
                id_sagreh INTEGER,
                nome TEXT,
                proprietario TEXT,
                gerencia TEXT,
                reg_hidrog TEXT,
                nome_municipio TEXT,
                nome_municipio_original TEXT,
                ini_monito TEXT,
                ano_constr INTEGER,
                o_barrad TEXT,
                ac_jusante INTEGER,
                id_ac_jus REAL,
                area_ha REAL,
                capacid_m3 REAL,
                cot_vert_m REAL,
                lg_vert_m REAL,
                cot_td_m TEXT,
                tipo_verte REAL,
                ri TEXT,
                wkt_geom TEXT
            )
            """
        )
        for r in RESERVATORIOS_ROWS:
            cur.execute(
                """
                INSERT INTO reservatorios_ceara (
                    id_sagreh, nome, proprietario, gerencia, reg_hidrog, nome_municipio,
                    nome_municipio_original, ini_monito, ano_constr, o_barrad, ac_jusante,
                    id_ac_jus, area_ha, capacid_m3, cot_vert_m, lg_vert_m, cot_td_m, tipo_verte,
                    ri, wkt_geom
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    r["id_sagreh"], r["nome"], r["proprietario"], r["gerencia"], r["reg_hidrog"],
                    r["nome_municipio"], r["nome_municipio_original"], r["ini_monito"], r["ano_constr"],
                    r["o_barrad"], r["ac_jusante"], r["id_ac_jus"], r["area_ha"], r["capacid_m3"],
                    r["cot_vert_m"], r["lg_vert_m"], r["cot_td_m"], r["tipo_verte"], r["ri"], r["wkt"],
                ],
            )

        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="session", autouse=True)
def _database():
    """Monta o banco SQLite+SpatiaLite descartável uma vez por sessão de teste."""
    _build_database()
    yield TEST_SQLITE_PATH
    shutil.rmtree(_TMP_DIR, ignore_errors=True)


@pytest.fixture(scope="session")
def app(_database):
    # Importado só agora (depois do banco existir e do ambiente configurado)
    # para garantir que `config.settings` já reflita as variáveis de teste.
    from data_service.main import app as fastapi_app

    return fastapi_app


@pytest.fixture(scope="session")
def client(app):
    with TestClient(app) as c:
        yield c


# ==========================================================================
# Fixtures de autenticação (JWT)
# ==========================================================================
def _make_token(secret: str, algorithm: str, **claims) -> str:
    return jwt.encode(claims, secret, algorithm=algorithm)


@pytest.fixture
def valid_token() -> str:
    from datetime import datetime, timedelta, timezone

    return _make_token(
        TEST_JWT_SECRET,
        TEST_JWT_ALGORITHM,
        sub="pytest-user",
        exp=datetime.now(timezone.utc) + timedelta(minutes=30),
    )


@pytest.fixture
def expired_token() -> str:
    from datetime import datetime, timedelta, timezone

    return _make_token(
        TEST_JWT_SECRET,
        TEST_JWT_ALGORITHM,
        sub="pytest-user",
        exp=datetime.now(timezone.utc) - timedelta(minutes=5),
    )


@pytest.fixture
def wrong_signature_token() -> str:
    from datetime import datetime, timedelta, timezone

    return _make_token(
        "esse-nao-e-o-segredo-certo",
        TEST_JWT_ALGORITHM,
        sub="pytest-user",
        exp=datetime.now(timezone.utc) + timedelta(minutes=30),
    )


@pytest.fixture
def malformed_token() -> str:
    return "isto-nao-e-um-jwt"


@pytest.fixture
def auth_headers(valid_token) -> dict:
    return {"Authorization": f"Bearer {valid_token}"}
