"""
Testes de regressão que documentam problemas reais encontrados ao montar
esta suíte contra o backend DATABASE_TYPE=sqlite (SQLite+SpatiaLite,
suportado por data_service/db.py). Não corrigidos aqui — ver instruções da
tarefa: reportar em vez de consertar por baixo dos panos em código de
produção. Nenhum destes problemas afeta o ambiente de produção atual, que
usa DATABASE_TYPE=postgres (.env), mas ambos tornam o backend sqlite
inutilizável hoje para os endpoints afetados.


ISSUE 1 (confirmado, quebra o endpoint inteiro): /geojson_assentamentos e
/geojson_reservatorios chamam a função SQL "ST_AsGeoJSON" incondicionalmente
(data_service/main.py, dentro de geojson_assentamentos() e
geojson_reservatorios()), sem checar settings.DATABASE_TYPE. No SpatiaLite
usado aqui (mesmo pacote `libsqlite3-mod-spatialite` do Debian bullseye que
o Dockerfile.tgdmserver instala), a função registrada chama-se apenas
"AsGeoJSON" -- "ST_AsGeoJSON" não existe, e o SQLite recusa a query já na
etapa de *preparação* do statement (antes de qualquer linha ser avaliada),
com "no such function: ST_AsGeoJSON". Na prática, sob DATABASE_TYPE=sqlite,
essas duas rotas retornam 500 para QUALQUER requisição, mesmo filtros que
não bateriam com nenhuma linha (o erro ocorre antes do WHERE ser avaliado).

Note que _geom_sql() (usada por /geojson e /geojson_muni) já trata esse
detalhe corretamente, alternando entre "AsGeoJSON"/"ST_Simplify" (sqlite) e
"ST_AsGeoJSON"/"ST_Simplify" (postgres) -- só /geojson_assentamentos e
/geojson_reservatorios não seguem esse mesmo padrão.


ISSUE 2 (confirmado para /geojson; mascarado pelo Issue 1 para
/geojson_assentamentos): tanto malha_fundiaria_ceara.geometry quanto
assentamentos_ceara.wkt_geometry são colunas TEXT no schema Postgres
produzido por importer_all.py (guardam uma string WKT, não um valor
geometry). /geojson e /geojson_muni chamam AsGeoJSON/ST_Simplify
diretamente sobre essas colunas, sem nenhum GeomFromText/ST_GeomFromText
explícito. Isso funciona no PostGIS porque ele registra um cast implícito
de text para geometry. O SpatiaLite não tem esse cast implícito:
AsGeoJSON('POLYGON(...)') sobre uma coluna TEXT não lança erro nenhum, só
retorna NULL silenciosamente (confirmado empiricamente). Como o código
descarta silenciosamente qualquer linha cujo geom_json seja "falsy", o
resultado é um 404 "não encontrado" mesmo havendo dados reais -- sem
nenhuma pista no log sobre a causa real. Confirmado abaixo para /geojson
com a tabela malha_fundiaria_ceara. Para assentamentos_ceara.wkt_geometry
esse mesmo problema existiria (mesmo formato de coluna), mas hoje nem chega
a se manifestar porque o Issue 1 já derruba a rota antes.

Em contraste, /geojson_reservatorios chama explicitamente
ST_GeomFromText(wkt_geom, 4326) antes de gerar o GeoJSON, então essa rota
NÃO tem o Issue 2 (só o Issue 1, que já é suficiente para quebrá-la).
"""


def test_geojson_assentamentos_returns_500_under_sqlite_backend(client, auth_headers):
    """Issue 1: ST_AsGeoJSON não existe no SpatiaLite -- qualquer requisição quebra."""
    resp = client.get(
        "/geojson_assentamentos", params={"municipio": "Fortaleza"}, headers=auth_headers
    )
    assert resp.status_code == 500, (
        "Se este teste começar a falhar (deixar de dar 500), o Issue 1 "
        "descrito no docstring deste módulo foi corrigido -- nesse caso, "
        "revisar/reativar os testes de happy-path em test_assentamentos.py "
        "e reavaliar o Issue 2 (que pode passar a se manifestar)."
    )


def test_geojson_reservatorios_returns_500_under_sqlite_backend(client, auth_headers):
    """Issue 1: mesma causa raiz, em /geojson_reservatorios."""
    resp = client.get(
        "/geojson_reservatorios", params={"municipio": "Fortaleza"}, headers=auth_headers
    )
    assert resp.status_code == 500, (
        "Se este teste começar a falhar (deixar de dar 500), o Issue 1 "
        "descrito no docstring deste módulo foi corrigido -- nesse caso, "
        "revisar/reativar os testes de happy-path em test_reservatorios.py."
    )


def test_geojson_returns_404_when_geometry_column_is_plain_wkt_text(client, auth_headers):
    """Issue 2: coluna geometry como TEXT WKT vira NULL silencioso -> 404."""
    # "MunicipioTextoWKT" tem um lote cuja coluna geometry recebeu uma
    # string WKT crua (não GeomFromText(...)), reproduzindo o shape real
    # de malha_fundiaria_ceara no Postgres (importer_all.py: "geometry TEXT").
    resp = client.get(
        "/geojson", params={"municipio": "MunicipioTextoWKT"}, headers=auth_headers
    )
    assert resp.status_code == 404, (
        "Se este teste começar a falhar (deixar de dar 404), é porque o "
        "Issue 2 descrito no docstring deste módulo foi corrigido -- nesse "
        "caso, atualize/remova este teste de regressão."
    )


def test_municipios_todos_still_lists_municipio_with_broken_geometry(client, auth_headers):
    # /municipios_todos não olha a coluna de geometria, então o município
    # aparece aqui mesmo sem GeoJSON gerável -- confirma que o dado existe
    # e o problema é especificamente na geração da geometria (Issue 2).
    resp = client.get("/municipios_todos", headers=auth_headers)
    assert resp.status_code == 200
    assert "MunicipioTextoWKT" in resp.json()["municipios"]


def test_assentamentos_municipios_still_lists_the_municipio_with_broken_geometry(client, auth_headers):
    # Idem para assentamentos: o dado existe (Issue 2 também se aplicaria
    # aqui), mas hoje o Issue 1 já derruba a rota de geometria antes disso.
    resp = client.get("/assentamentos_municipios", headers=auth_headers)
    assert resp.status_code == 200
    assert "MunicipioTextoWKT" in resp.json()["municipios"]
