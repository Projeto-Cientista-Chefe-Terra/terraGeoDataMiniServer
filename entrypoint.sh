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
python import_data_to_postgres.py

echo "▶ Carregando dados dos Assentamentos para o banco de dados..."
python import_data_assentamentos_to_postgres.py

echo "▶ Carregando dados dos Reservatórios para o banco de dados..."
python import_data_reservatorios_to_postgres.py

echo "▶ Executando Terra Geodata Mini-Server..."

echo "🚀  Iniciando Gunicorn..."
exec gunicorn data_service.main:app \
     --worker-class uvicorn.workers.UvicornWorker \
     --bind 0.0.0.0:8000 \
     --workers "${GUNICORN_WORKERS}" \
     --threads "${GUNICORN_THREADS}" \
     --log-level "${GUNICORN_LOG_LEVEL}"