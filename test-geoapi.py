import requests
import json

url = "http://geoapi.idace.ce.gov.br/geoapi/pessoa/municipio/VICOSA%20DO%20CEARA?pagina=0&tamanho=1000&ordenarPor=proprietario" #QUIXADA

payload = {}
headers = {
  'Authorization': 'Bearer ***TOKEN_GEOAPI_REMOVIDO***'
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()
data_str = json.dumps(data, indent=4, ensure_ascii=False)
print(data_str)
print(f"Total de registros: {data_str.count('numero')}")

print(f"Total de registros: {len(data)}")
