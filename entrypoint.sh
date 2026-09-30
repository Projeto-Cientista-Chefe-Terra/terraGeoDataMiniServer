#!/bin/bash
# Entrypoint do terraGeoDataMiniServer: carga dos CSVs (opcional) e gunicorn.

set -euo pipefail

echo "🚀 ENTRYPOINT script executando...$(date)"

if [ -f .env ]; then
    echo "▶ Carregando variáveis do .env"
    export $(grep -v '^#' .env | xargs)
else
    echo "⚠️  Arquivo .env não encontrado. Usando variáveis de ambiente."
fi

# Cria diretório para SQLite se necessário
if [ "${DATABASE_TYPE:-postgres}" == "sqlite" ] && [ ! -d "$(dirname "${SQLITE_PATH:-data/geodata.sqlite}")" ]; then
    mkdir -p "$(dirname "${SQLITE_PATH:-data/geodata.sqlite}")"
fi

# A carga troca o conteúdo de cada tabela numa única transação, então rodar
# de novo a cada reinício não duplica registros. Para desligar (por exemplo,
# quando a carga passar para o Carregador de Dados): IMPORTAR_DADOS_NA_INICIALIZACAO=false
if [ "${IMPORTAR_DADOS_NA_INICIALIZACAO:-true}" == "true" ]; then
    echo "▶ Carregando dados para o banco de dados..."
    python importer_all.py
else
    echo "▶ Importação na inicialização desativada."
fi

if [ -d "datasets" ]; then
    echo "▶ Removendo pasta 'datasets'..."
    rm -rf datasets
fi

echo "🚀  Iniciando Gunicorn..."
exec gunicorn data_service.main:app \
     --worker-class uvicorn.workers.UvicornWorker \
     --bind "${TGDMSERVER_HOST:-0.0.0.0}:${TGDMSERVER_PORT:-8000}" \
     --workers "${GUNICORN_WORKERS:-4}" \
     --threads "${GUNICORN_THREADS:-8}" \
     --timeout "${GUNICORN_TIMEOUT:-120}" \
     --log-level "${GUNICORN_LOG_LEVEL:-info}"
