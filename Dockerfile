# Dockerfile

FROM python:3.12-slim

# Define diretório de trabalho
WORKDIR /tgdmserver

# Não gerar bytecode .pyc e deixar logs fluindo no stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_ROOT_USER_ACTION=ignore \
    GUNICORN_WORKERS=4 \
    GUNICORN_THREADS=8

# Cria diretorios /tgdmserver e /tgdmserver/upload. Define suas permissoes
    RUN mkdir -p /tgdmserver /tgdmserver/upload \
    && chown -R 1000:1000 /tgdmserver \
    && chown -R 1000:1000 /tgdmserver/upload \
    && chmod -R 664 /tgdmserver/upload


# Instala dependências do sistema (GDAL, Spatialite, PostgreSQL dev, etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gdal-bin libgdal-dev python3-gdal \
    sqlite3 libsqlite3-mod-spatialite \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências Python (incluindo Gunicorn)
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Copia código-fonte da aplicação
COPY . .

# Expõe a porta do FastAPI
EXPOSE 8000

# Inicia Gunicorn com UvicornWorker para rodar em produção
# CMD ["gunicorn", "data_service.main:app", \
#      "--worker-class", "uvicorn.workers.UvicornWorker", \
#      "--bind", "0.0.0.0:8000", \
#      "--workers", "${GUNICORN_WORKERS}", \
#      "--threads", "${GUNICORN_THREADS}", \
#      "--log-level", "info"]

COPY entrypoint.sh /tgdmserver/entrypoint.sh
RUN chmod +x /tgdmserver/entrypoint.sh

# Entry-point
ENTRYPOINT ["/tgdmserver/entrypoint.sh"]