# terraGeoDataMiniServer

Este micro‐serviço entrega dados da malha fundiária (geometrias e propriedades) armazenados em um banco SQLite/Spatialite.

## Principais arquivos

- **data_service/main.py**: endpoints FastAPI:

  - `GET /regioes`
  - `GET /municipios?regiao=<NOME_DA_REGIAO>`
  - `GET /geojson?regiao=<NOME_DA_REGIAO>`
  - `GET /geojson?municipio=<NOME_DO_MUNICIPIO>`

- **data_service/db.py**: configura conexão com Postgres/PostGIS. Lê credenciais via `.env` ou `os.getenv`.

- **data_service/utils.py**: funções auxiliares (conversão de linha em Feature GeoJSON, limpeza de NaN, etc).
- **data/terra_data.sqlite**: banco de dados SQLite/Spatialite.
   (Instalar o sqlite3 e o sqlite3-spatialite para usar o banco de dados)

## Como usar o mini-servidor?

1. Instale dependências (recomenda-se criar um virtualenv)

```bash
pip install -r requirements.txt
```

2. Carregue os dados CSV no PostGIS (veja `load_into_postgis.py` ou use seu próprio método).

3. Inicie o microserviço

```bash
uvicorn data_service.main:app --reload
```

4. Endpoints

* http://127.0.0.1:8000/regioes
* http://127.0.0.1:8000/municipios?regiao=NomeDaRegiao
* http://127.0.0.1:8000/geojson?regiao=NomeDaRegiao
* http://127.0.0.1:8000/geojson?municipio=NomeDoMunicipio


## Quais libs uso?


* fastapi: framework web para criar endpoints REST de forma simples e performática.

* uvicorn[standard]: servidor ASGI para rodar o FastAPI.

* psycopg2-binary: driver para conectar ao Postgres/PostGIS.

* python‐manticorepath (ou simplesmente "pydantic", pois o FastAPI já depende dele, mas deixamos aqui para reforçar)

* python‐decouple (opcional, mas recomendado): para ler variáveis de ambiente (usuário, senha, host do DB, etc.) de um arquivo .env.
```