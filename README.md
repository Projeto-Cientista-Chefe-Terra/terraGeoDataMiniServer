# Terra Geodata Mini-Server

Serdidor de dados geoespaciais que opera com um conjunto de micro‑serviçod com a finalidade de entregar dados da malha fundiária (geometrias e propriedades) do Ceará, armazenados em SQLite/SpatiaLite ou Postgres/PostGIS.

Ele fornece:

* Listagem de regiões administrativas e municípios
* GeoJSON simplificado para regiões/municípios
* Dados tabulares de lotes fundiários (sem geometria)
* Health check do serviço

Baseado em **FastAPI**, **SQLAlchemy** e extensões geoespaciais (SpatiaLite ou PostGIS).

---

## Instalação e Pré‑requisitos

1. **Clone** o repositório:

   ```bash
   git clone https://github.com/usuario/terraGeoDataMiniServer.git
   cd terraGeoDataMiniServer/data_service
   ```
2. **Ambiente Python** (recomendado):

   ```bash
   python -m venv .venv && source .venv/bin/activate
   ```
3. **Instale dependências**:

   ```bash
   pip install -r requirements.txt
   ```

   **Principais libs**:

   * `fastapi` e `uvicorn[standard]` (ASGI server)
   * `sqlalchemy`, `psycopg2-binary` (Postgres/PostGIS)
   * `spatialite` (para SQLite/SpatiaLite)
   * `python-decouple` ou similar para ler `.env`
   * `apscheduler` (pré-processamento)
   * `python-json-logger` (log JSON)
4. **Banco de dados**:

   * SQLite/SpatiaLite: arquivo em `data/terra_data.sqlite`
   * Postgres/PostGIS: configure `DATABASE_TYPE=postgres` e `postgres_dsn` no `.env`
5. **Carregamento de CSV** (opcional): use `load_into_postgis.py` ou seu próprio método para popular o banco.

---

## Configuração

Centralizada em `config/settings.py` ou via `.env`:

| Variável                 | Descrição                                 |
| ------------------------ | ----------------------------------------- |
| `DATABASE_TYPE`          | `sqlite` ou `postgres`                    |
| `SQLITE_PATH`            | Caminho para arquivo SQLite               |
| `postgres_dsn`           | DSN de conexão PostgreSQL (URL)           |
| `TABLE_DADOS_FUNDIARIOS` | Nome da tabela de dados fundiários        |
| `TABLE_GEOM_MUNICIPIOS`  | Nome da tabela de geometria de municípios |
| `CSV_INPUT_PATH`         | Pasta ou arquivo CSV de origem (se usado) |

---

## Estrutura de Módulos

### `db.py`

* `get_sqlalchemy_engine()`: retorna engine para SQLite/SpatiaLite ou PostgreSQL/PostGIS.

### `utils.py`

* `safe_value(val)`: converte `NaN` para `None` (JSON válido).
* `row_to_feature(mapping)`: converte `RowMapping` em feature GeoJSON.

### `main.py`

* Define app FastAPI e middlewares (CORS, GZip, Brotli).
* Lifespan: testa conexão (`SELECT 1`), inicia `BackgroundScheduler` para pré‑processar GeoJSON (diariamente às 02:00).
* Dependências injetadas com `@lru_cache()` para performance.

---

## Endpoints REST

> **Base URL:** `http://<host>:<port>/`

| Endpoint            | Método | Parâmetros             | Descrição                                   |
| ------------------- | ------ | ---------------------- | ------------------------------------------- |
| `/health`           | GET    | —                      | Health check (`{ status: "healthy" }`)      |
| `/regioes`          | GET    | —                      | Lista todas regiões                         |
| `/municipios`       | GET    | `regiao=<nome>`        | Municípios por região                       |
| `/municipios_todos` | GET    | —                      | Lista todos municípios                      |
| `/geojson_muni`     | GET    | `municipio=<nome>`     | GeoJSON simplificado de um município        |
| `/geojson`          | GET    | `regiao=<nome>` **ou** | GeoJSON simplificado de região ou município |
|                     |        | `municipio=<nome>`     | (parâmetros `tolerance`, `limit` opcionais) |
| `/dados_fundiarios` | GET    | `regiao=<nome>` **ou** | Dados tabulares de lotes (sem geometria)    |
|                     |        | `municipio=<nome>`     |                                             |

**Códigos de erro**: 400 para parâmetros inválidos; 404 para não encontrado.

---


## Testes Automatizados

Coloque seu `testes_api.sh` no diretório raiz e dê permissão:

```bash
chmod +x testes_api.sh
./testes_api.sh
```

Ele verifica status 200, JSON válido e campos obrigatórios em todos os endpoints principais.

Segue o códitgo do arquivo `testes_api.sh`:


```bash
#!/bin/bash

API_BASE="http://localhost:8000"
REGIAO_ESPECIFICA="Cariri"  # Região específica para testes detalhados

echo "1. Testando health check..."
curl -s "${API_BASE}/health" | jq .

echo -e "\n2. Testando listagem de regiões..."
regiao=$(curl -s "${API_BASE}/regioes" | jq -r '.regioes[0]')
echo "Primeira região encontrada: $regiao"

echo -e "\n3. Testando listagem de municípios para a região '$regiao'..."
municipio=$(curl -s "${API_BASE}/municipios?regiao=${regiao}" | jq -r '.municipios[0]')
echo "Primeiro município encontrado: $municipio"

echo -e "\n4. Testando obtenção de GeoJSON para a região '$regiao'..."
curl -s "${API_BASE}/geojson?regiao=${regiao}" | jq '.features | length'

echo -e "\n5. Testando obtenção de GeoJSON para o município '$municipio'..."
curl -s "${API_BASE}/geojson?municipio=${municipio}" | jq '.features | length'

echo -e "\n6. Testando obtenção de dados fundiários para a região '$regiao'..."
curl -s "${API_BASE}/dados_fundiarios?regiao=${regiao}" | jq 'length'

echo -e "\n7. Testando obtenção de dados fundiários para o município '$municipio'..."
curl -s "${API_BASE}/dados_fundiarios?municipio=${municipio}" | jq 'length'

echo -e "\n8. Testando erros de validação..."
echo "Faltando parâmetros:"
curl -s "${API_BASE}/geojson" | jq

echo "Parâmetros conflitantes:"
curl -s "${API_BASE}/geojson?regiao=${regiao}&municipio=${municipio}" | jq

echo -e "\n9. TESTES ESPECÍFICOS PARA A REGIÃO DO CARIRI"

echo -e "\n9.1 Verificando se a região '${REGIAO_ESPECIFICA}' existe..."
curl -s "${API_BASE}/regioes" | jq ".regioes | map(select(. == \"${REGIAO_ESPECIFICA}\")) | length"

echo -e "\n9.2 Obtendo lista de municípios do ${REGIAO_ESPECIFICA}..."
curl -s "${API_BASE}/municipios?regiao=${REGIAO_ESPECIFICA}" | jq

echo -e "\n9.3 Obtendo GeoJSON da região ${REGIAO_ESPECIFICA}..."
curl -s "${API_BASE}/geojson?regiao=${REGIAO_ESPECIFICA}" | jq '.features[0].properties'

echo -e "\n9.4 Obtendo dados fundiários completos do ${REGIAO_ESPECIFICA}..."
curl -s "${API_BASE}/dados_fundiarios?regiao=${REGIAO_ESPECIFICA}" | jq '.[0]'

echo -e "\n9.5 Resumo estatístico das propriedades do ${REGIAO_ESPECIFICA}:"
curl -s "${API_BASE}/dados_fundiarios?regiao=${REGIAO_ESPECIFICA}" | jq '
  {
    total_propriedades: length,
    area_total: (map(.area) | add),
    area_media: (map(.area) | add / length),
    modulos_fiscais: (map(.modulo_fiscal) | unique),
    situacoes_juridicas: (map(.situacao_juridica) | group_by(.) | map({(.[0]): length})),
    municipios: (map(.nome_municipio) | unique | length)
  }'

```
---

## Alguns pontos importantes de sua arquitetura

* **Cache**: `@lru_cache` em funções de listagem para melhorar performance.
* **Pré-processamento**: agendado via APScheduler para gerar arquivos em `data/geodata`.
* **CORS**: habilitado para `*` (em produção restrinja).
* **Logs**: formato JSON para fácil ingestão em sistemas de observabilidade.
* **Escalonabilidade**: use Gunicorn/UVicorn em cluster e contêineres Docker (veja `docker-compose.yml`).


