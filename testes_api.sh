#!/bin/bash

# Configurações
API_BASE="http://localhost:8000"
REGIAO_ESPECIFICA="CARIRI"

# Cores para feedback visual
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Verificar dependências (Fedora-specific)
check_dependencies() {
    local missing=()
    
    if ! command -v curl &> /dev/null; then
        missing+=("curl")
    fi
    
    if ! command -v jq &> /dev/null; then
        missing+=("jq")
    fi
    
    if ! command -v dialog &> /dev/null; then
        missing+=("dialog")
    fi
    
    if [ ${#missing[@]} -gt 0 ]; then
        echo -e "${RED}Erro: Dependências não encontradas:${NC} ${missing[*]}"
        echo -e "Instale com:"
        echo -e "sudo dnf install ${missing[*]}"
        exit 1
    fi
}

# Função para fazer requisições com tratamento de erro
api_request() {
    local url="$1"
    local description="$2"
    
    echo -e "\n${BLUE}${description}${NC}"
    echo -e "URL: ${YELLOW}${url}${NC}"
    
    local start_time=$(date +%s.%N)
    local response=$(curl -s -w "\nHTTP_CODE:%{http_code}" "${url}")
    local http_code=$(echo "$response" | grep "HTTP_CODE:" | cut -d':' -f2)
    local content=$(echo "$response" | sed '/HTTP_CODE:/d')
    local elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)
    
    if [ -z "$http_code" ]; then
        echo -e "${RED}✖ Erro: Não foi possível conectar à API${NC}"
        return 1
    fi
    
    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
        echo -e "${GREEN}✔ Sucesso (HTTP ${http_code})${NC} - Tempo: ${elapsed}s"
        echo "$content" | jq .
    else
        echo -e "${RED}✖ Erro (HTTP ${http_code})${NC} - Tempo: ${elapsed}s"
        echo "$content" | jq . 2>/dev/null || echo "$content"
    fi
    
    return 0
}

# Testes Gerais da API
testes_api_geral() {
    clear
    echo -e "${BLUE}=== TESTES GERAIS DA API ===${NC}"
    
    api_request "${API_BASE}/health" "1. Health Check"
    
    api_request "${API_BASE}/regioes" "2. Listagem de regiões"
    local regiao=$(curl -s "${API_BASE}/regioes" | jq -r '.regioes[0]')
    
    api_request "${API_BASE}/municipios?regiao=${regiao}" "3. Listagem de municípios para a região '${regiao}'"
    local municipio=$(curl -s "${API_BASE}/municipios?regiao=${regiao}" | jq -r '.municipios[0]')
    
    read -p $'\nPressione Enter para voltar ao menu...'
}

# Testes GeoJSON
testes_geojson() {
    clear
    echo -e "${BLUE}=== TESTES GEOJSON ===${NC}"
    
    local regiao=$(curl -s "${API_BASE}/regioes" | jq -r '.regioes[0]')
    local municipio=$(curl -s "${API_BASE}/municipios?regiao=${regiao}" | jq -r '.municipios[0]')
    
    api_request "${API_BASE}/geojson?regiao=${regiao}" "4. GeoJSON para região '${regiao}'"
    
    api_request "${API_BASE}/geojson?municipio=${municipio}" "5. GeoJSON para município '${municipio}'"
    
    api_request "${API_BASE}/geojson_muni?municipio=${municipio}" "6. GeoJSON via /geojson_muni para '${municipio}'"
    
    # Teste com município inexistente
    local municipio_fake="THIS_DOES_NOT_EXIST"
    api_request "${API_BASE}/geojson_muni?municipio=${municipio_fake}" "7. Teste com município inexistente"
    
    # Testes de validação
    api_request "${API_BASE}/geojson" "8. Teste sem parâmetros"
    api_request "${API_BASE}/geojson?regiao=${regiao}&municipio=${municipio}" "9. Teste com parâmetros conflitantes"
    
    read -p $'\nPressione Enter para voltar ao menu...'
}

# Testes Dados Fundiários
testes_dados_fundiarios() {
    clear
    echo -e "${BLUE}=== TESTES DADOS FUNDIÁRIOS ===${NC}"
    
    local regiao=$(curl -s "${API_BASE}/regioes" | jq -r '.regioes[0]')
    local municipio=$(curl -s "${API_BASE}/municipios?regiao=${regiao}" | jq -r '.municipios[0]')
    
    api_request "${API_BASE}/dados_fundiarios?regiao=${regiao}" "10. Dados fundiários para região '${regiao}'"
    
    api_request "${API_BASE}/dados_fundiarios?municipio=${municipio}" "11. Dados fundiários para município '${municipio}'"
    
    read -p $'\nPressione Enter para voltar ao menu...'
}

# Testes para Região Específica
testes_regiao_cariri() {
    clear
    echo -e "${BLUE}=== TESTES REGIÃO ${REGIAO_ESPECIFICA} ===${NC}"
    
    api_request "${API_BASE}/regioes" "12. Verificando existência da região '${REGIAO_ESPECIFICA}'"
    
    api_request "${API_BASE}/municipios?regiao=${REGIAO_ESPECIFICA}" "13. Lista de municípios de ${REGIAO_ESPECIFICA}"
    
    api_request "${API_BASE}/geojson?regiao=${REGIAO_ESPECIFICA}" "14. GeoJSON da região ${REGIAO_ESPECIFICA}"
    
    api_request "${API_BASE}/dados_fundiarios?regiao=${REGIAO_ESPECIFICA}" "15. Dados fundiários da região ${REGIAO_ESPECIFICA}"
    
    read -p $'\nPressione Enter para voltar ao menu...'
}

# Testes Assentamentos
testes_assentamentos() {
    clear
    echo -e "${BLUE}=== TESTES ASSENTAMENTOS ===${NC}"
    
    # 1. Listar municípios disponíveis
    api_request "${API_BASE}/assentamentos_municipios" "1. Listagem de municípios com assentamentos"
    
    # Obter a lista de municípios
    local municipios=$(curl -s "${API_BASE}/assentamentos_municipios" | jq -r '.municipios[]')
    local primeiro_municipio=$(echo "$municipios" | head -n 1)
    
    # 2. Mostrar todos os assentamentos de um município (apenas contagem)
    if [ -n "$primeiro_municipio" ]; then
        echo -e "\n${BLUE}2. Obtendo assentamentos para o município '${primeiro_municipio}'${NC}"
        
        # Obter todos os assentamentos do município
        local assentamentos_resp=$(curl -s "${API_BASE}/geojson_assentamentos?municipio=${primeiro_municipio}")
        local total_assentamentos=$(echo "$assentamentos_resp" | jq '.features | length')
        
        echo -e "Total de assentamentos em ${primeiro_municipio}: ${GREEN}${total_assentamentos}${NC}"
        
        # 3. Mostrar detalhes de um assentamento específico
        if [ "$total_assentamentos" -gt 0 ]; then
            echo -e "\n${BLUE}3. Detalhes do primeiro assentamento em '${primeiro_municipio}'${NC}"
            
            # Selecionar o primeiro assentamento
            local primeiro_assentamento=$(echo "$assentamentos_resp" | jq '.features[0]')
            
            # Mostrar informações relevantes
            echo "$primeiro_assentamento" | jq '{
                tipo: .type,
                propriedades: .properties | {
                    nome: .nome_assentamento,
                    municipio: .nome_municipio,
                    area: .area_ha,
                    fase: .fase,
                    modulo_fiscal: .modulo_fiscal,
                    familias: .qtd_familias
                },
                geometria: .geometry.type
            }'
            
            # Mostrar coordenadas da geometria (resumidas)
            local coords_count=$(echo "$primeiro_assentamento" | jq '.geometry.coordinates | length')
            echo -e "\nResumo da geometria:"
            echo "$primeiro_assentamento" | jq '{ 
                tipo_geometria: .geometry.type,
                total_coordenadas: (.geometry.coordinates | length),
                primeira_coordenada: .geometry.coordinates[0][0] 
            }'
        fi
    else
        echo -e "${RED}Nenhum município com assentamentos encontrado.${NC}"
    fi
    
    # 4. Teste com município inexistente
    local municipio_fake="blaublau"
    api_request "${API_BASE}/geojson_assentamentos?municipio=${municipio_fake}" "4. Teste com município inexistente"
    
    # 5. Teste de todos os assentamentos de todos os municípios
    echo -e "\n${BLUE}5. Teste de todos os assentamentos de todos os municípios${NC}"
    local todos_assentamentos_resp=$(curl -s "${API_BASE}/geojson_assentamentos?municipio=todos")
    local total_todos_assentamentos=$(echo "$todos_assentamentos_resp" | jq '.features | length')
    echo -e "Total de assentamentos em todos os municípios: ${GREEN}${total_todos_assentamentos}${NC}"
    
    if [ "$total_todos_assentamentos" -gt 0 ]; then
        echo -e "\nResumo do primeiro assentamento de todos os municípios:"
        echo "$todos_assentamentos_resp" | jq '.features[0] | {
            tipo: .type,
            propriedades: .properties | {
                nome: .nome_assentamento,
                municipio: .nome_municipio
            },
            geometria: .geometry.type
        }'
    fi
    
    # 6. Teste de simplificação geométrica (opcional)
    if [ -n "$primeiro_municipio" ] && [ "$total_assentamentos" -gt 0 ]; then
        echo -e "\n${BLUE}6. Teste de simplificação geométrica para um assentamento${NC}"
        api_request "${API_BASE}/geojson_assentamentos?municipio=${primeiro_municipio}&tolerance=0.01&decimals=4" "Assentamento simplificado"
    fi
    
    read -p $'\nPressione Enter para voltar ao menu...'
}


# Testes Reservatórios
testes_reservatorios() {
    clear
    echo -e "${BLUE}=== TESTES RESERVATÓRIOS ===${NC}"
    
    # 1. Listar municípios disponíveis
    api_request "${API_BASE}/reservatorios_municipios" "1. Listagem de municípios com reservatórios"
    
    # Obter a lista de municípios
    local municipios=$(curl -s "${API_BASE}/reservatorios_municipios" | jq -r '.municipios[]')
    local primeiro_municipio=$(echo "$municipios" | head -n 1)
    
    # 2. Mostrar todos os reservatórios de um município (apenas contagem)
    if [ -n "$primeiro_municipio" ]; then
        echo -e "\n${BLUE}2. Obtendo reservatórios para o município '${primeiro_municipio}'${NC}"
        
        # Obter todos os reservatórios do município
        local reservatorios_resp=$(curl -s "${API_BASE}/geojson_reservatorios?municipio=${primeiro_municipio}")
        local total_reservatorios=$(echo "$reservatorios_resp" | jq '.features | length')
        
        echo -e "Total de reservatórios em ${primeiro_municipio}: ${GREEN}${total_reservatorios}${NC}"
        
        # 3. Mostrar detalhes de um reservatório específico
        if [ "$total_reservatorios" -gt 0 ]; then
            echo -e "\n${BLUE}3. Detalhes do primeiro reservatório em '${primeiro_municipio}'${NC}"
            
            # Selecionar o primeiro reservatório
            local primeiro_reservatorio=$(echo "$reservatorios_resp" | jq '.features[0]')
            
            # Mostrar informações relevantes
            echo "$primeiro_reservatorio" | jq '{
                tipo: .type,
                propriedades: .properties | {
                    nome: .nome,
                    municipio: .nome_municipio,
                    capacidade: .capacid_m3,
                    area: .area_ha,
                    proprietario: .proprietario,
                    ano_construcao: .ano_constr
                },
                geometria: .geometry.type
            }'
            
            # Mostrar coordenadas da geometria (resumidas)
            local coords_count=$(echo "$primeiro_reservatorio" | jq '.geometry.coordinates | length')
            echo -e "\nResumo da geometria:"
            echo "$primeiro_reservatorio" | jq '{ 
                tipo_geometria: .geometry.type,
                total_coordenadas: (.geometry.coordinates | length),
                primeira_coordenada: .geometry.coordinates[0][0] 
            }'
        fi
    else
        echo -e "${RED}Nenhum município com reservatórios encontrado.${NC}"
    fi
    
    # 4. Teste com município inexistente
    local municipio_fake="blaublau"
    api_request "${API_BASE}/geojson_reservatorios?municipio=${municipio_fake}" "4. Teste com município inexistente"
    
    # 5. Teste de todos os reservatórios de todos os municípios
    echo -e "\n${BLUE}5. Teste de todos os reservatórios de todos os municípios${NC}"
    local todos_reservatorios_resp=$(curl -s "${API_BASE}/geojson_reservatorios?municipio=todos")
    local total_todos_reservatorios=$(echo "$todos_reservatorios_resp" | jq '.features | length')
    echo -e "Total de reservatórios em todos os municípios: ${GREEN}${total_todos_reservatorios}${NC}"
    
    if [ "$total_todos_reservatorios" -gt 0 ]; then
        echo -e "\nResumo do primeiro reservatório de todos os municípios:"
        echo "$todos_reservatorios_resp" | jq '.features[0] | {
            tipo: .type,
            propriedades: .properties | {
                nome: .nome,
                municipio: .nome_municipio
            },
            geometria: .geometry.type
        }'
    fi
    
    # 6. Teste de simplificação geométrica (opcional)
    if [ -n "$primeiro_municipio" ] && [ "$total_reservatorios" -gt 0 ]; then
        echo -e "\n${BLUE}6. Teste de simplificação geométrica para um reservatório${NC}"
        api_request "${API_BASE}/geojson_reservatorios?municipio=${primeiro_municipio}&tolerance=0.01&decimals=4" "Reservatório simplificado"
    fi
    
    read -p $'\nPressione Enter para voltar ao menu...'
}


# Menu principal
main_menu() {
    check_dependencies
    
    while true; do
        escolha=$(dialog --clear \
            --backtitle "Menu de Testes API Fundiária - Fedora Workstation 42" \
            --title "Escolha uma categoria de testes" \
            --menu "Selecione a categoria:" 20 60 9 \
            1 "Testes Gerais da API" \
            2 "Testes GeoJSON" \
            3 "Testes Dados Fundiários" \
            4 "Testes Região ${REGIAO_ESPECIFICA}" \
            5 "Testes Assentamentos" \
            6 "Testes Reservatórios" \
            0 "Sair" \
            2>&1 >/dev/tty)

        clear
        case $escolha in
            1) testes_api_geral ;;
            2) testes_geojson ;;
            3) testes_dados_fundiarios ;;
            4) testes_regiao_cariri ;;
            5) testes_assentamentos ;;
            6) testes_reservatorios ;;
            0) break ;;
            *) echo -e "${RED}Opção inválida!${NC}"; sleep 1;;
        esac
    done
    
    clear
    echo -e "${GREEN}Testador automatizado por escript finalizado!${NC}"
    echo -e "${YELLOW}May the Force be with Bash!${NC}"
    echo -e "${YELLOW}Fedora Workstation 42${NC}"
}

# Iniciar o menu
main_menu