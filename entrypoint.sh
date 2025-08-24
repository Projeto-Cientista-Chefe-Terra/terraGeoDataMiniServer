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

# echo "▶ Carregando dados para o banco de dados..."
# python import_data_to_postgres.py

# echo "▶ Carregando dados dos Assentamentos para o banco de dados..."
# python import_data_assentamentos_to_postgres.py

# echo "▶ Carregando dados dos Reservatórios para o banco de dados..."
# python import_data_reservatorios_to_postgres.py

# Importers de dados 
# echo "▶ Carregando dados da malha fundiária do Ceará para o banco de dados..."
# python importer_malha_fundiaria_ceara.py

# echo "▶ Carregando dados de municípios, regiões administrativas e módulos fiscais para o banco de dados..."
# python importer_regioes_adm_municipios_mf.py

# echo "▶ Carregando dados dos Reservatórios Monitorados para o banco de dados..."
# python importer_reservatorios_monitorados.py

# echo "▶ Carregando dados dos Assentamentos do Ceará para o banco de dados..."
# python importer_assentamentos.py

# echo "▶ Carregando dados dos municípios do Ceará para o banco de dados..."
# python importer_municipios_ceara.py

echo "▶ Carregando dados para o banco de dados..."
python importer_all.py




# Remove pastas não necessárias
if [ -d "data" ]; then
    echo "▶ Removendo pasta 'data'..."
    rm -rf data
fi

# if [ -d "datasets" ]; then
#     echo "▶ Removendo pasta 'datasets'..."
#     rm -rf datasets
# fi

echo "▶ Executando Terra Geodata Mini-Server..."

echo "🚀  Iniciando Gunicorn..."


exec gunicorn data_service.main:app \
     --worker-class uvicorn.workers.UvicornWorker \
     --bind ${TGDMSERVER_HOST}:${TGDMSERVER_PORT} \
     --workers "${GUNICORN_WORKERS}" \
     --threads "${GUNICORN_THREADS}" \
     --log-level "${GUNICORN_LOG_LEVEL}"



