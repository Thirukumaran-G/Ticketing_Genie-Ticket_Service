"""Email service — ticket-service."""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class EmailService:

    def __init__(self) -> None:
        self._host: str = settings.SMTP_HOST
        self._port: int = settings.SMTP_PORT
        self._username: str = settings.SMTP_USERNAME
        self._password: str = settings.SMTP_PASSWORD
        self._from_email: str = settings.SMTP_FROM_EMAIL
        self._from_name: str = settings.SMTP_FROM_NAME

    def _build_credentials_html(
        self,
        full_name: str | None,
        email: str,
        temp_password: str,
        user_type: str,
        agent_skill: str | None = None,
    ) -> str:
        display = full_name or email.split("@")[0].replace(".", " ").title()
        role = user_type.replace("_", " ").title()

        skill_row = ""
        if agent_skill:
            skill_row = f"""
            <tr>
              <td style="padding:10px 0;border-bottom:1px solid #e5e7eb;
                         font-size:13px;color:#6b7280;width:38%;">Assigned Skill</td>
              <td style="padding:10px 0;border-bottom:1px solid #e5e7eb;
                         font-size:13px;color:#111827;">{agent_skill}</td>
            </tr>"""

        return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#f9fafb;
             font-family:'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0"
         style="padding:40px 0;background:#f9fafb;">
    <tr>
      <td align="center">
        <table width="520" cellpadding="0" cellspacing="0"
               style="background:#ffffff;border-radius:10px;
                      border:1px solid #e5e7eb;padding:40px;">
          <tr>
            <td>

              <!-- Welcome -->
              <p style="margin:0 0 4px;font-size:22px;font-weight:700;color:#111827;">
                Hi {display},
              </p>
              <p style="margin:0 0 28px;font-size:15px;color:#6b7280;line-height:1.6;">
                Welcome to the Support Platform. Your <strong
                style="color:#111827;">{role}</strong> account is ready.
              </p>

              <!-- Credentials table -->
              <table width="100%" cellpadding="0" cellspacing="0"
                     style="border-top:1px solid #e5e7eb;">
                <tr>
                  <td style="padding:10px 0;border-bottom:1px solid #e5e7eb;
                             font-size:13px;color:#6b7280;width:38%;">Role</td>
                  <td style="padding:10px 0;border-bottom:1px solid #e5e7eb;
                             font-size:13px;color:#111827;">{role}</td>
                </tr>
                {skill_row}
                <tr>
                  <td style="padding:10px 0;border-bottom:1px solid #e5e7eb;
                             font-size:13px;color:#6b7280;">Email</td>
                  <td style="padding:10px 0;border-bottom:1px solid #e5e7eb;
                             font-size:13px;color:#111827;">{email}</td>
                </tr>
                <tr>
                  <td style="padding:14px 0 4px;font-size:13px;color:#6b7280;">
                    Temporary Password
                  </td>
                  <td style="padding:14px 0 4px;">
                    <span style="font-family:'Courier New',monospace;font-size:16px;
                                 font-weight:700;color:#111827;background:#f3f4f6;
                                 padding:6px 14px;border-radius:5px;
                                 letter-spacing:1.5px;">
                      {temp_password}
                    </span>
                  </td>
                </tr>
              </table>

              <!-- Note -->
              <p style="margin:24px 0 0;font-size:13px;color:#9ca3af;line-height:1.6;">
                You will be asked to change your password on first login.
                If you did not expect this email, contact your administrator.
              </p>

            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

    def _send(self, to_email: str, subject: str, html_body: str) -> None:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{self._from_name} <{self._from_email}>"
        msg["To"] = to_email
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(self._host, self._port) as server:
                server.ehlo()
                server.starttls()
                server.login(self._username, self._password)
                server.sendmail(self._from_email, to_email, msg.as_string())
            logger.info("credential_email_sent", to=to_email)
        except smtplib.SMTPException as exc:
            logger.error("credential_email_failed", to=to_email, error=str(exc))

    def send_agent_credentials(
        self,
        to_email: str,
        full_name: str | None,
        temp_password: str,
        user_type: str,
        agent_skill: str | None = None,
    ) -> None:
        """Send temp credentials — called via BackgroundTasks. Never log temp_password."""
        html = self._build_credentials_html(
            full_name, to_email, temp_password, user_type, agent_skill
        )
        self._send(
            to_email=to_email,
            subject="Your Support Platform Credentials",
            html_body=html,
        )