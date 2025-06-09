# Dockerfile sugerido
# -------------------
FROM python:3.12-slim

WORKDIR /app

# Instala o GDAL e dependências do sistema
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

EXPOSE 8000

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
