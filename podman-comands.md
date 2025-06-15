podman network create \
  --subnet 192.168.100.0/24 \
  --gateway 192.168.100.1 \
  terra_network
terra_network

podman-compose build --no-cache

podman-compose logs -f app

podman-compose logs -f postgres

podman-compose exec app bash

podman-compose build

podman-compose down && podman-compose up -d

podman container prune # remove containers não usados
podman image prune # remove imagens não usadas
podman volume prune #remove volumes orfaos
podman system prune --all --volumes # remove containers, imagens e volumes nao usados

alias podman-fresh='podman system prune -f --all --volumes && podman up --build'
