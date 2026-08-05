#!/bin/bash
# Builda e publica a imagem do terraGeoDataMiniServer no Docker Hub.
#
# Uso: ./publish-docker.sh <versao>
# Ex.: ./publish-docker.sh 1.2.0
#
# Pré-requisito: estar logado no Docker Hub nesta máquina.
#   podman login docker.io -u wellingtonwfsarmento
# (este script nunca guarda usuário/senha/token — usa a sessão já autenticada)

set -euo pipefail

IMAGE="wellingtonwfsarmento/terra-geodata-mini-server"
VERSION="${1:?Uso: ./publish-docker.sh <versao, ex: 1.2.0>}"

cd "$(dirname "$0")"

if ! podman login --get-login docker.io >/dev/null 2>&1; then
  echo "Não logado no Docker Hub. Rode primeiro:" >&2
  echo "  podman login docker.io -u wellingtonwfsarmento" >&2
  exit 1
fi

echo "==> Build: $IMAGE:$VERSION"
podman build -f Dockerfile.tgdmserver -t "$IMAGE:$VERSION" -t "$IMAGE:latest" .

echo "==> Push: $IMAGE:$VERSION"
podman push "$IMAGE:$VERSION"

echo "==> Push: $IMAGE:latest"
podman push "$IMAGE:latest"

echo "==> Publicado: $IMAGE:$VERSION e $IMAGE:latest"
