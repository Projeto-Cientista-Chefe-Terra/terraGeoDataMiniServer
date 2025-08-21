import json

# Nome do arquivo
arquivo = "BANABUIU.json"

# Abrir e carregar o JSON
with open(arquivo, "r", encoding="utf-8") as f:
    dados = json.load(f)

# Se o arquivo contém uma lista de objetos JSON
if isinstance(dados, list):
    print(f"Número de objetos JSON no arquivo: {len(dados)}")

# Caso seja um objeto com chaves que contenham listas
elif isinstance(dados, dict):
    # Exemplo: se os objetos estão dentro de uma chave chamada "imoveis"
    for chave, valor in dados.items():
        if isinstance(valor, list):
            print(f"Número de objetos na chave '{chave}': {len(valor)}")
else:
    print("O arquivo JSON não está em um formato esperado (lista ou objeto com listas).")
