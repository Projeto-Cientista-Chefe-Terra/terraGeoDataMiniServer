#!/bin/bash
set -euo pipefail # Modo estrito de execução

# Isso falhará imediatamente se:
# 1. O python falhar (-e)
# 2. Usar variável não definida (-u)
# 3. Falhar em qualquer parte da pipeline (-o pipefail)

# echo "▶ Importando dados para o banco..."
# python import_data_postgres.py  | tee -a import_log.txt
# python import_data_sqlite.py  | tee -a import_log.txt

echo "▶ Executando Terra Geodata Mini-Server..."
# exec uvicorn data_service.main:app --host 0.0.0.0 --port 8000 #usa o uvicorn - rapido mais com um só Worker

# Usa o gunicorn - servidor com pre-fork, multi-process
echo "🚀  Iniciando Gunicorn..."
exec gunicorn data_service.main:app \
     --worker-class uvicorn.workers.UvicornWorker \
     --bind 0.0.0.0:8000 \
     --workers "${GUNICORN_WORKERS}" \
     --threads "${GUNICORN_THREADS}" \
     --log-level info
