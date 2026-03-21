"""Small wrapper around optional secret encryption for provider session blobs."""

from __future__ import annotations

import base64

from cryptography.fernet import Fernet, InvalidToken


class SecretBox:
    def __init__(self, key: str | None):
        self._fernet = Fernet(key.encode("utf-8")) if key else None

    @property
    def enabled(self) -> bool:
        return self._fernet is not None

    def seal(self, value: str | None) -> str | None:
        if not value:
            return None
        if self._fernet is not None:
            token = self._fernet.encrypt(value.encode("utf-8")).decode("utf-8")
            return f"fernet:{token}"
        token = base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")
        return f"plain:{token}"

    def open(self, payload: str | None) -> str | None:
        if not payload:
            return None
        if payload.startswith("plain:"):
            encoded = payload.split(":", 1)[1]
            return base64.urlsafe_b64decode(encoded.encode("utf-8")).decode("utf-8")
        if payload.startswith("fernet:"):
            if self._fernet is None:
                raise RuntimeError("APP_ENCRYPTION_KEY is required to decrypt stored secrets.")
            token = payload.split(":", 1)[1]
            try:
                return self._fernet.decrypt(token.encode("utf-8")).decode("utf-8")
            except InvalidToken as exc:
                raise RuntimeError("Stored secrets could not be decrypted with the current APP_ENCRYPTION_KEY.") from exc
        raise RuntimeError("Unsupported secret payload format.")
