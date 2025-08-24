# Cria as pastas para os certificados SSL caso não existam
mkdir -p nginx/ssl
mkdir -p ssl
mkdir -p ../dashboard_fundiario_ceara/ssl

# Cria os certificados para o NGNIX

# Certificado para tgdmserver.virtual.ufc.br
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout nginx/ssl/tgdmserver.key.pem \
    -out nginx/ssl/tgdmserver.cert.pem \
    -subj "/C=BR/ST=Ceara/L=Fortaleza/O=UFC/CN=tgdmserver.virtual.ufc.br"


# Certificado para terrace.virtual.ufc.br
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout nginx/ssl/terrace.key.pem \
    -out   nginx/ssl/terrace.cert.pem \
    -subj "/C=BR/ST=Ceara/L=Fortaleza/O=UFC/CN=terrace.virtual.ufc.br"



# Copia os certificados para as pastas SSL dos serviços

cp nginx/ssl/tgdmserver.cert.pem ssl/tgdmserver.cert.pem
cp nginx/ssl/tgdmserver.cert.pem ../dashboard_fundiario_ceara/ssl/tgdmserver.cert.pem


cp nginx/ssl/terrace.cert.pem ssl/terrace.cert.pem
cp nginx/ssl/terrace.cert.pem   ../dashboard_fundiario_ceara/ssl/terrace.cert.pem