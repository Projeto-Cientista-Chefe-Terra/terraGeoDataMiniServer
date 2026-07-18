def test_health_check_returns_200_and_healthy_status(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


def test_health_check_does_not_require_auth(client):
    # /health não depende de verify_token: deve funcionar sem Authorization.
    resp = client.get("/health", headers={})
    assert resp.status_code == 200
