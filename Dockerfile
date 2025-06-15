# Dockerfile
# -----------

FROM python:3.12-slim

WORKDIR /tgdmserver

# Cria diretório e define permissões
RUN mkdir -p /app && chown -R 1000:1000 /app

# Instala GDAL, Spatialite e SQLite
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    sqlite3 \
    libsqlite3-mod-spatialite \
  && rm -rf /var/lib/apt/lists/*

# Instala as dependências Python
COPY requirements.txt .
ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# Copia o código e define dono
COPY . .

# Expõe a porta do FastAPI
EXPOSE 8000

COPY entrypoint.sh /tgdmserver/entrypoint.sh
RUN chmod +x /tgdmserver/entrypoint.sh

# Entry-point
ENTRYPOINT ["/tgdmserver/entrypoint.sh"]