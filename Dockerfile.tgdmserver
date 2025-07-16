# Dockerfile do tgdmserver

FROM python:3.13-slim-bullseye

LABEL maintainer="Wellington Wagner F. Sarmento <wwagner@virtual.ufc.br>" \
      description="Dockerfile do Terra.Ce"

WORKDIR /tgdmserver

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_ROOT_USER_ACTION=ignore \
    GUNICORN_WORKERS=4 \
    GUNICORN_THREADS=8 \
    TZ=America/Fortaleza

# Instala dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    libgeos-dev \
    libproj-dev \
    libpq-dev \
    tzdata \
    sqlite3 \
    libsqlite3-mod-spatialite && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN mkdir -p /tgdmserver/upload \
    && chown -R 1000:1000 /tgdmserver/upload \
    && chmod -R 664 /tgdmserver/upload

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

COPY . .

EXPOSE 8000

COPY entrypoint.sh /tgdmserver/entrypoint.sh
RUN chmod +x /tgdmserver/entrypoint.sh

ENTRYPOINT ["/tgdmserver/entrypoint.sh"]