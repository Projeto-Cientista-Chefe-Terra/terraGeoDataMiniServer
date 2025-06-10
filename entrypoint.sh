#!/bin/bash
set -e

# echo "▶ Importando dados para o banco..."
python import_data_postgres.py

echo "▶ Iniciando a aplicação FastAPI..."
exec uvicorn data_service.main:app --host 0.0.0.0 --port 8000
