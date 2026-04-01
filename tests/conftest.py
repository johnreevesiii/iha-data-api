"""Shared test fixtures."""

import os
import jwt
import pytest
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient


# Set test env vars before importing app
os.environ["JWT_SECRET"] = "test-secret-key"
os.environ["ALLOWED_ORIGINS"] = "*"

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)


def _make_token(
    user_id="test-user-001",
    tier="free",
    assigned_fips="40109",
    assigned_state="OK",
    expired=False,
):
    """Generate a test JWT."""
    exp = datetime.now(timezone.utc) + (
        timedelta(hours=-1) if expired else timedelta(hours=1)
    )
    payload = {
        "userId": user_id,
        "tier": tier,
        "assignedFips": assigned_fips,
        "assignedState": assigned_state,
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, os.environ["JWT_SECRET"], algorithm="HS256")


@pytest.fixture
def free_token():
    return _make_token(tier="free", assigned_fips="40109")


@pytest.fixture
def premium_token():
    return _make_token(tier="premium")


@pytest.fixture
def internal_token():
    return _make_token(tier="internal")


@pytest.fixture
def expired_token():
    return _make_token(expired=True)


@pytest.fixture
def free_headers(free_token):
    return {"Authorization": f"Bearer {free_token}"}


@pytest.fixture
def premium_headers(premium_token):
    return {"Authorization": f"Bearer {premium_token}"}


@pytest.fixture
def internal_headers(internal_token):
    return {"Authorization": f"Bearer {internal_token}"}
