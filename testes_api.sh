#!/bin/bash

API_BASE="http://localhost:8000"
REGIAO_ESPECIFICA="Cariri"  # Região específica para testes detalhados

echo "1. Testando health check..."
curl -s "${API_BASE}/health" | jq .

echo -e "\n2. Testando listagem de regiões..."
regiao=$(curl -s "${API_BASE}/regioes" | jq -r '.regioes[0]')
echo "Primeira região encontrada: $regiao"

echo -e "\n3. Testando listagem de municípios para a região '$regiao'..."
municipio=$(curl -s "${API_BASE}/municipios?regiao=${regiao}" | jq -r '.municipios[0]')
echo "Primeiro município encontrado: $municipio"

echo -e "\n4. Testando obtenção de GeoJSON para a região '$regiao'..."
curl -s "${API_BASE}/geojson?regiao=${regiao}" | jq '.features | length'

echo -e "\n5. Testando obtenção de GeoJSON para o município '$municipio'..."
curl -s "${API_BASE}/geojson?municipio=${municipio}" | jq '.features | length'

echo -e "\n6. Testando obtenção de dados fundiários para a região '$regiao'..."
curl -s "${API_BASE}/dados_fundiarios?regiao=${regiao}" | jq 'length'

echo -e "\n7. Testando obtenção de dados fundiários para o município '$municipio'..."
curl -s "${API_BASE}/dados_fundiarios?municipio=${municipio}" | jq 'length'

echo -e "\n8. Testando erros de validação..."
echo "Faltando parâmetros:"
curl -s "${API_BASE}/geojson" | jq

echo "Parâmetros conflitantes:"
curl -s "${API_BASE}/geojson?regiao=${regiao}&municipio=${municipio}" | jq

echo -e "\n9. TESTES ESPECÍFICOS PARA A REGIÃO DO CARIRI"

echo -e "\n9.1 Verificando se a região '${REGIAO_ESPECIFICA}' existe..."
curl -s "${API_BASE}/regioes" | jq ".regioes | map(select(. == \"${REGIAO_ESPECIFICA}\")) | length"

echo -e "\n9.2 Obtendo lista de municípios do ${REGIAO_ESPECIFICA}..."
curl -s "${API_BASE}/municipios?regiao=${REGIAO_ESPECIFICA}" | jq

echo -e "\n9.3 Obtendo GeoJSON da região ${REGIAO_ESPECIFICA}..."
curl -s "${API_BASE}/geojson?regiao=${REGIAO_ESPECIFICA}" | jq '.features[0].properties'

echo -e "\n9.4 Obtendo dados fundiários completos do ${REGIAO_ESPECIFICA}..."
curl -s "${API_BASE}/dados_fundiarios?regiao=${REGIAO_ESPECIFICA}" | jq '.[0]'

echo -e "\n9.5 Resumo estatístico das propriedades do ${REGIAO_ESPECIFICA}:"
curl -s "${API_BASE}/dados_fundiarios?regiao=${REGIAO_ESPECIFICA}" | jq '
  {
    total_propriedades: length,
    area_total: (map(.area) | add),
    area_media: (map(.area) | add / length),
    modulos_fiscais: (map(.modulo_fiscal) | unique),
    situacoes_juridicas: (map(.situacao_juridica) | group_by(.) | map({(.[0]): length})),
    municipios: (map(.nome_municipio) | unique | length)
  }'