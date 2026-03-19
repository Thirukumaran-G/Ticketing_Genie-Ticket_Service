"""
src/core/services/gcs_service.py
=================================
GCS wrapper for ticket attachment storage.

Upload  → stores PUBLIC URL in DB (file_path column)
          e.g. https://storage.googleapis.com/{bucket}/{blob_path}

View    → converts public URL back to blob path → generates signed URL
          (30 min validity) → frontend redirects to it
"""
from __future__ import annotations

import uuid
import logging
from pathlib import Path

from src.config.settings import settings

logger = logging.getLogger(__name__)

_bucket = None


def _get_bucket():
    global _bucket
    if _bucket is not None:
        return _bucket
    try:
        from google.cloud import storage
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'google-cloud-storage'. "
            "Run: uv add google-cloud-storage"
        ) from exc

    client  = storage.Client(project=settings.GCS_PROJECT_ID)
    _bucket = client.bucket(settings.GCS_BUCKET_NAME)
    logger.info(f"GCS bucket initialised: {settings.GCS_BUCKET_NAME}")
    return _bucket


def _public_url(blob_path: str) -> str:
    """Return the permanent public HTTPS URL for a blob path."""
    return f"https://storage.googleapis.com/{settings.GCS_BUCKET_NAME}/{blob_path}"


def _blob_path_from_public_url(public_url_or_blob_path: str) -> str:
    """
    Extract blob path from a public GCS URL.
    https://storage.googleapis.com/{bucket}/{blob_path}  →  {blob_path}
    If already a raw blob path, return as-is (backward compat).
    """
    prefix = f"https://storage.googleapis.com/{settings.GCS_BUCKET_NAME}/"
    if public_url_or_blob_path.startswith(prefix):
        return public_url_or_blob_path[len(prefix):]
    return public_url_or_blob_path


def upload_attachment(
    file_bytes: bytes,
    filename:   str,
    folder:     str,
    mime_type:  str = "application/octet-stream",
) -> str:
    """
    Upload bytes to GCS under:
      {GCS_BUCKET_PREFIX}/attachments/{folder}/{uuid}_{filename}

    Returns the PUBLIC URL — stored in DB as file_path.
    e.g. https://storage.googleapis.com/{bucket}/{blob_path}

    folder example: "tickets/{ticket_id}"
    """
    safe_name   = Path(filename).name.replace(" ", "_")[:100]
    unique_name = f"{uuid.uuid4().hex}_{safe_name}"
    blob_path   = f"{settings.GCS_BUCKET_PREFIX}/attachments/{folder}/{unique_name}"

    bucket = _get_bucket()
    blob   = bucket.blob(blob_path)
    blob.cache_control = "private, max-age=3600"
    blob.upload_from_string(file_bytes, content_type=mime_type)

    public_url = _public_url(blob_path)
    logger.info(f"GCS upload: {blob_path} ({len(file_bytes)} bytes, {mime_type})")
    return public_url   # ← this is what gets stored in DB


def get_signed_url(public_url_or_blob_path: str, expiration_minutes: int = 30) -> str:
    """
    Generate a time-limited signed URL for viewing/downloading an attachment.

    Accepts either:
      - public URL   (https://storage.googleapis.com/{bucket}/{blob_path})
      - raw blob path ({GCS_BUCKET_PREFIX}/attachments/...)

    Requires the Cloud Run service account to have
    roles/iam.serviceAccountTokenCreator on GCS_TARGET_SERVICE_ACCOUNT.
    """
    import datetime
    from google.auth import impersonated_credentials
    import google.auth

    blob_path    = _blob_path_from_public_url(public_url_or_blob_path)
    base_creds, _project = google.auth.default()

    target_creds = impersonated_credentials.Credentials(
        source_credentials=base_creds,
        target_principal=settings.GCS_TARGET_SERVICE_ACCOUNT,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=300,
    )

    blob = _get_bucket().blob(blob_path)
    url  = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(minutes=expiration_minutes),
        method="GET",
        credentials=target_creds,
    )
    logger.info(f"GCS signed URL generated: {blob_path} (valid {expiration_minutes} min)")
    return url