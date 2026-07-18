"""
Testes da dependência de autenticação JWT (verify_token em data_service/main.py),
usando /regioes como endpoint representativo (qualquer endpoint protegido
serviria: todos usam a mesma Depends(verify_token)).
"""


def test_no_authorization_header_is_rejected(client):
    resp = client.get("/regioes")
    # HTTPBearer(auto_error=True) na versão instalada de FastAPI (0.128.5)
    # recusa com 401 "Not authenticated" quando não há header Authorization
    # nenhum (fastapi.security.http.HTTPBearer.make_not_authenticated_error).
    # Isso difere do comportamento de versões mais antigas do FastAPI, que
    # usavam 403 nesse caso -- vale reconferir se o FastAPI for atualizado.
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Not authenticated"


def test_malformed_token_returns_401(client, malformed_token):
    resp = client.get("/regioes", headers={"Authorization": f"Bearer {malformed_token}"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Token inválido"


def test_expired_token_returns_401(client, expired_token):
    resp = client.get("/regioes", headers={"Authorization": f"Bearer {expired_token}"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Token expirado"


def test_wrong_signature_token_returns_401(client, wrong_signature_token):
    resp = client.get("/regioes", headers={"Authorization": f"Bearer {wrong_signature_token}"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Token inválido"


def test_valid_token_is_accepted(client, auth_headers):
    resp = client.get("/regioes", headers=auth_headers)
    assert resp.status_code == 200


def test_wrong_auth_scheme_is_rejected(client, valid_token):
    # Esquema "Basic" em vez de "Bearer": HTTPBearer deve recusar.
    resp = client.get("/regioes", headers={"Authorization": f"Basic {valid_token}"})
    assert resp.status_code == 401
