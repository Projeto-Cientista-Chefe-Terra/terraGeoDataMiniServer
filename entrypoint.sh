#!/bin/bash
set -euo pipefail # Modo estrito de execução

# Isso falhará imediatamente se:
# 1. O python falhar (-e)
# 2. Usar variável não definida (-u)
# 3. Falhar em qualquer parte da pipeline (-o pipefail)

# echo "▶ Importando dados para o banco..."
# python import_data_postgres.py  | tee -a import_log.txt
# python import_data_sqlite.py  | tee -a import_log.txt

echo "▶ Iniciando a aplicação FastAPI..."
exec uvicorn data_service.main:app --host 0.0.0.0 --port 8000
