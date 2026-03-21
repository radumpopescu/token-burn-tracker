"""Admin authentication helpers for settings and manual polling endpoints."""

from __future__ import annotations

import os
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

basic_auth = HTTPBasic(auto_error=False)


def admin_auth_enabled() -> bool:
    return bool(os.environ.get("ADMIN_PASSWORD"))


def require_admin(credentials: HTTPBasicCredentials | None = Depends(basic_auth)) -> None:
    expected_password = os.environ.get("ADMIN_PASSWORD")
    expected_username = os.environ.get("ADMIN_USERNAME", "admin")

    if not expected_password:
        return

    if credentials is None:
        raise _auth_error()

    username_ok = secrets.compare_digest(credentials.username, expected_username)
    password_ok = secrets.compare_digest(credentials.password, expected_password)
    if not (username_ok and password_ok):
        raise _auth_error()


def _auth_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Admin authentication required.",
        headers={"WWW-Authenticate": "Basic"},
    )
