#!/bin/bash

API_BASE="http://localhost:8000"
REGIAO_ESPECIFICA="CARIRI"  # Região específica para testes detalhados. Use nome todo em maiúsculo

echo "1. Testando health check..."
curl -s "${API_BASE}/health" | jq .

echo -e "\n2. Testando listagem de regiões..."
regiao=$(curl -s "${API_BASE}/regioes" | jq -r '.regioes[0]')
echo "Primeira região encontrada: $regiao"

echo -e "\n3. Testando listagem de municípios para a região '$regiao'..."
municipio=$(curl -s "${API_BASE}/municipios?regiao=${regiao}" | jq -r '.municipios[0]')
echo "Primeiro município encontrado: $municipio"


echo -e "\n4. Testando obtenção de GeoJSON para a região '$regiao' (via /geojson)..."
features_regiao=$(curl -s "${API_BASE}/geojson?regiao=${regiao}" | jq '.features | length')
echo "Número de features retornadas: $features_regiao"


# read -p "Pressione Enter para continuar..."

echo -e "\n5. Testando obtenção de GeoJSON para o município '$municipio' (via /geojson)..."
features_muni_generic=$(curl -s "${API_BASE}/geojson?municipio=${municipio}" | jq '.features | length')
echo "Features via /geojson?municipio: $features_muni_generic"


# read -p "Pressione Enter para continuar..."

echo -e "\n6. Testando obtenção de GeoJSON para o município '$municipio' (via /geojson_muni)..."
geojson_muni_resp=$(curl -s "${API_BASE}/geojson_muni?municipio=${municipio}")
features_muni_especial=$(echo "$geojson_muni_resp" | jq '{
  count:      (.features | length),
  first_geom: .features[0].geometry,
  first_props:.features[0].properties
}')
echo "Resumo da primeira feature:"
echo "$features_muni_especial" | jq


# read -p "Pressione Enter para continuar..."

echo -e "\n6.1 Verificando tipo de geometria da primeira feature de '$municipio'..."
tipo_geom=$(echo "$geojson_muni_resp" | jq -r '.features[0].geometry.type')
echo "Tipo de geometria: $tipo_geom"
if [[ "$tipo_geom" != "Polygon" && "$tipo_geom" != "MultiPolygon" ]]; then
  echo "ERRO: Tipo de geometria inesperado!"
  exit 1
fi

echo -e "\n6.2 Verificando se o nome do município bate com a consulta..."
nome_props=$(echo "$geojson_muni_resp" | jq -r '.features[0].properties.nome_municipio')
echo "Nome no GeoJSON: $nome_props"
if [[ "${nome_props,,}" != "${municipio,,}" ]]; then
  echo "ERRO: Nome do município retornado não bate com o consultado!"
  exit 1
fi

# read -p "Pressione Enter para continuar..."

echo -e "\n6.3 Testando resposta para município inexistente (esperado: erro 404)..."
municipio_fake="THIS_DOES_NOT_EXIST"
codigo_http=$(curl -s -o /dev/null -w "%{http_code}" "${API_BASE}/geojson_muni?municipio=${municipio_fake}")
echo "Código HTTP para município inexistente: $codigo_http"
if [[ "$codigo_http" != "404" ]]; then
  echo "ERRO: Esperado HTTP 404 para município inexistente!"
  exit 1
fi

# read -p "Pressione Enter para continuar..."

echo -e "\n7. Testando obtenção de dados fundiários para a região '$regiao'..."
count_df_reg=$(curl -s "${API_BASE}/dados_fundiarios?regiao=${regiao}" | jq 'length')
echo "Total de lotes fundiários na região: $count_df_reg"

echo -e "\n8. Testando obtenção de dados fundiários para o município '$municipio'..."
count_df_muni=$(curl -s "${API_BASE}/dados_fundiarios?municipio=${municipio}" | jq 'length')
echo "Total de lotes fundiários no município: $count_df_muni"

# read -p "Pressione Enter para continuar..."

echo -e "\n9. Testando erros de validação em /geojson"
echo "- Faltando parâmetros:"
curl -s "${API_BASE}/geojson" | jq
echo "- Parâmetros conflitantes:"
curl -s "${API_BASE}/geojson?regiao=${regiao}&municipio=${municipio}" | jq

# read -p "Pressione Enter para continuar..."

echo -e "\n10. TESTES ESPECÍFICOS PARA A REGIÃO DO CARIRI"
echo "10.1 Verificando existência da região '${REGIAO_ESPECIFICA}'..."
curl -s "${API_BASE}/regioes" | jq ".regioes | index(\"${REGIAO_ESPECIFICA}\")"

# read -p "Pressione Enter para continuar..."

echo -e "\n10.2 Lista de municípios de ${REGIAO_ESPECIFICA}..."
curl -s "${API_BASE}/municipios?regiao=${REGIAO_ESPECIFICA}" | jq

# read -p "Pressione Enter para continuar..."

echo -e "\n10.3 GeoJSON da região ${REGIAO_ESPECIFICA} (features e primeira geometry)..."
curl -s "${API_BASE}/geojson?regiao=${REGIAO_ESPECIFICA}" | \
  jq '{
    total:         (.features | length),
    primeira_geom: .features[0].geometry,
    primeira_props:.features[0].properties
  }'


# read -p "Pressione Enter para continuar..."


echo -e "\n10.4 Dados fundiários completos do ${REGIAO_ESPECIFICA} (primeiro registro)..."
curl -s "${API_BASE}/dados_fundiarios?regiao=${REGIAO_ESPECIFICA}" | jq '.[0]'


# read -p "Pressione Enter para continuar..."

echo -e "\n10.5 Resumo estatístico das propriedades do ${REGIAO_ESPECIFICA}:"
curl -s "${API_BASE}/dados_fundiarios?regiao=${REGIAO_ESPECIFICA}" | jq '
  {
    total_propriedades: length,
    area_total:         (map(.area) | add),
    area_media:         (map(.area) | add / length),
    modulos_fiscais:    (map(.modulo_fiscal) | unique),
    situacoes_juridicas:(map(.situacao_juridica) | group_by(.) | map({(.[0]): length})),
    municipios:         (map(.nome_municipio) | unique | length)
  }'
