"""Smoke tests for health check and auth."""


def test_health_check(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["service"] == "iha-data-api"


def test_community_requires_auth(client):
    r = client.get("/v1/community/40109")
    assert r.status_code == 401


def test_community_rejects_expired(client, expired_token):
    r = client.get(
        "/v1/community/40109",
        headers={"Authorization": f"Bearer {expired_token}"},
    )
    assert r.status_code == 401


def test_free_tier_blocked_from_other_fips(client, free_headers):
    """Free user assigned to 40109 should be blocked from 06037."""
    r = client.get("/v1/community/06037", headers=free_headers)
    assert r.status_code == 403


def test_premium_can_access_any_fips(client, premium_headers):
    """Premium user can access any FIPS (may return empty data but not 403)."""
    r = client.get("/v1/community/06037", headers=premium_headers)
    assert r.status_code == 200


def test_hospitals_requires_auth(client):
    r = client.get("/v1/hospitals?lat=35.4&lon=-97.5&radius=30")
    assert r.status_code == 401


def test_demographics_requires_auth(client):
    r = client.get("/v1/demographics/40/109")
    assert r.status_code == 401


def test_hpsa_requires_auth(client):
    r = client.get("/v1/hpsa/OK/Oklahoma")
    assert r.status_code == 401
