import requests
import json

url = "http://geoapi.idace.ce.gov.br/geoapi/pessoa/municipio/IRAUCUBA?pagina=0&tamanho=10000&ordenarPor=proprietario" #QUIXADA

payload = {}
headers = {
  'Authorization': 'Bearer ***TOKEN_GEOAPI_REMOVIDO***'
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()

print(f"Total de registros: {len(data)}")
