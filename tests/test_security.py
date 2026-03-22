from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from fastapi.security import HTTPBasicCredentials

from token_burn.security import admin_auth_enabled, require_admin


class SecurityTests(unittest.TestCase):
    def test_admin_auth_disabled_when_password_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(admin_auth_enabled())
            self.assertIsNone(require_admin(None))

    def test_admin_auth_accepts_matching_credentials(self) -> None:
        credentials = HTTPBasicCredentials(username="admin", password="secret")
        with patch.dict(
            os.environ,
            {"ADMIN_USERNAME": "admin", "ADMIN_PASSWORD": "secret"},
            clear=True,
        ):
            self.assertTrue(admin_auth_enabled())
            self.assertIsNone(require_admin(credentials))


if __name__ == "__main__":
    unittest.main()
