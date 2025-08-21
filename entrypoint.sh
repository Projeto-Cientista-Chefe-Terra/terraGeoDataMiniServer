#!/bin/bash

set -euo pipefail


echo "🚀 ENTRYPOINT script executando...$(date)"

# Verifica se o .env existe
if [ ! -f .env ]; then
    echo "⚠️  Arquivo .env não encontrado. Usando variáveis de ambiente padrão."
else
    echo "▶ Carregando variáveis do .env"
    export $(grep -v '^#' .env | xargs)
fi

# Cria diretório para SQLite se necessário
if [ "${DATABASE_TYPE:-postgres}" == "sqlite" ] && [ ! -d "$(dirname "${SQLITE_PATH:-data/geodata.sqlite}")" ]; then
    mkdir -p "$(dirname "${SQLITE_PATH:-data/geodata.sqlite}")"
fi

echo "▶ Carregando dados para o banco de dados..."
python import_data_to_postgres_neo.py

# echo "▶ Carregando dados para o banco de dados..."
# python import_data_to_postgres.py

# echo "▶ Carregando dados dos Assentamentos para o banco de dados..."
# python import_data_assentamentos_to_postgres.py

# echo "▶ Carregando dados dos Reservatórios para o banco de dados..."
# python import_data_reservatorios_to_postgres.py

# Remove pastas não necessárias
if [ -d "data" ]; then
    echo "▶ Removendo pasta 'data'..."
    rm -rf data
fi

if [ -d "datasets" ]; then
    echo "▶ Removendo pasta 'data'..."
    rm -rf datasets
fi

echo "▶ Executando Terra Geodata Mini-Server..."

echo "🚀  Iniciando Gunicorn..."

# # caminhos no container (monte via volume/secret)
# : "${SSL_CERT_FILE:=/run/certs/fullchain.pem}"
# : "${SSL_KEY_FILE:=/run/certs/privkey.pem}"

# exec gunicorn data_service.main:app \
#   --worker-class uvicorn.workers.UvicornWorker \
#   --bind 0.0.0.0:8000 \
#   --workers "${GUNICORN_WORKERS:-2}" \
#   --threads "${GUNICORN_THREADS:-2}" \
#   --log-level "${GUNICORN_LOG_LEVEL:-info}" \
#   --certfile "$SSL_CERT_FILE" \
#   --keyfile "$SSL_KEY_FILE"

exec gunicorn data_service.main:app \
     --worker-class uvicorn.workers.UvicornWorker \
     --bind 0.0.0.0:8000 \
     --workers "${GUNICORN_WORKERS}" \
     --threads "${GUNICORN_THREADS}" \
     --log-level "${GUNICORN_LOG_LEVEL}"
