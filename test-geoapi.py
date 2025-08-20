import requests
import json

url = "http://geoapi.idace.ce.gov.br/geoapi/pessoa/municipio/QUIXADA?pagina=0&tamanho=10000&ordenarPor=proprietario"  # QUIXADA
payload = {}
headers = {
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJJREFDRSIsImlhdCI6MTc1NTE4NTc4MSwiZXhwIjoxNzg2NzQzMzgxfQ.y_qILJ6Kj474a8lP_DcT3EWt70Rqys30ltHKWclZCE4'
}

response = requests.get(url, headers=headers, data=payload)
data = response.json()

print(f"Total de registros recebidos: {len(data)}")

data_str = json.dumps(data, indent=4, ensure_ascii=False)
print(f"Total de registros convertidos para string: {data_str.count('numero')}")


null_count = sum(1 for record in data if record is None)


unique_records = []
duplicates = []

for record in data:
    if record is None:  
        continue
    if record in unique_records:
        duplicates.append(record)
    else:
        unique_records.append(record)


with open("duplicados.json", "w", encoding="utf-8") as f:
    json.dump(duplicates, f, indent=4, ensure_ascii=False)

print(f"Total de registros null: {null_count}")
print(f"Total de registros duplicados (ignorando null): {len(duplicates)}")
print("Arquivo 'duplicados.json' gerado com os registros duplicados.")
