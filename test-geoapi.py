import requests
import json

url = "http://geoapi.idace.ce.gov.br/geoapi/pessoa/municipio/VICOSA%20DO%20CEARA?pagina=0&tamanho=1000&ordenarPor=proprietario" #QUIXADA

payload = {}
headers = {
  'Authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJJREFDRSIsImlhdCI6MTc1NTE4NTc4MSwiZXhwIjoxNzg2NzQzMzgxfQ.y_qILJ6Kj474a8lP_DcT3EWt70Rqys30ltHKWclZCE4'
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()
data_str = json.dumps(data, indent=4, ensure_ascii=False)
print(data_str)
print(f"Total de registros: {data_str.count('numero')}")

print(f"Total de registros: {len(data)}")
