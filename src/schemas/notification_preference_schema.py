from __future__ import annotations

from pydantic import BaseModel


class NotificationPreferenceResponse(BaseModel):
    user_id: str
    preferred_contact: str  # "email" | "in_app"

    model_config = {"from_attributes": True}