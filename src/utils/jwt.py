# src/core/security/jwt.py

from __future__ import annotations

from typing import Any, Dict

from jose import jwt
from jose.exceptions import ExpiredSignatureError, JWTError

from src.config.settings import settings


def decode_token(token: str) -> Dict[str, Any]:
    """
    Decode and verify a JWT access token.
    Raises ValueError on any failure (expired, tampered, wrong algo, etc.)
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,      
            algorithms=[settings.JWT_ALGORITHM],
        )
        return payload
    except ExpiredSignatureError:
        raise ValueError("Token has expired.")
    except JWTError as e:
        raise ValueError(f"Invalid token: {e}")