from __future__ import annotations

import email
import imaplib
import os
import smtplib
from email.message import EmailMessage
from email.utils import parseaddr
from typing import Any

from sqlalchemy import select

from app.core.config import settings
from app.db.base import SessionLocal
from app.models.acquisition_supervisor import AcquisitionProspect
from app.services.relay_money_optimizer_patch import optimized_auto_reply_text


_applied = False
_original_outreach_status = None


def _send_direct_smtp(to_email: str, subject: str, plain_text: str, html_body: str) -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    mailboxes = outreach._smtp_mailboxes()
    if not mailboxes:
        raise RuntimeError("No SMTP mailboxes configured")

    host = os.getenv("COLD_SMTP_HOST", "smtp.porkbun.com").strip()
    port = int(os.getenv("COLD_SMTP_PORT", "587").strip() or "587")
    security = os.getenv("COLD_SMTP_SECURITY", "starttls").strip().lower()
    reply_to = os.getenv("COLD_SMTP_REPLY_TO", settings.reply_to_email or "").strip()
    failures: list[dict[str, str]] = []
    last_exc: Exception | None = None

    for mailbox in sorted(mailboxes, key=lambda m: m.slot):
        message = EmailMessage()
        message["From"] = mailbox.address
        message["To"] = to_email
        message["Subject"] = subject
        if reply_to:
            message["Reply-To"] = reply_to
        message.set_content(plain_text)
        message.add_alternative(
            f"<div style='font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:1.6'>{html_body}</div>",
            subtype="html",
        )

        server: smtplib.SMTP | smtplib.SMTP_SSL | None = None
        try:
            if security == "ssl":
                server = smtplib.SMTP_SSL(host, port, timeout=30)
            else:
                server = smtplib.SMTP(host, port, timeout=30)
            server.ehlo()
            if security == "starttls":
                server.starttls()
                server.ehlo()
            server.login(mailbox.address, mailbox.password)
            rejected = server.sendmail(mailbox.address, [to_email], message.as_string())
            if isinstance(rejected, dict) and to_email in rejected:
                raise RuntimeError(f"SMTP recipient rejected: {to_email}")
        except Exception as exc:
            last_exc = exc
            failures.append(
                {
                    "sender_address": mailbox.address,
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:500],
                }
            )
            continue
        finally:
            if server is not None:
                try:
                    server.quit()
                except Exception:
                    pass

        return {
            "provider": "smtp",
            "sender_address": mailbox.address,
            "smtp_host": host,
            "smtp_port": port,
            "smtp_security": security,
            "ignored_campaign_cap": True,
            "reply_autoclose_cap_bypass": True,
            "smtp_failover_attempts": failures,
            "smtp_mailboxes_available": len(mailboxes),
            "status": "sent",
        }

    failure_summary = "; ".join(
        f"{failure['sender_address']}:{failure['error_type']}" for failure in failures
    )
    raise RuntimeError(f"All reply auto-close SMTP mailboxes failed: {failure_summary}") from last_exc


def optimized_poll_reply_mailbox(limit: int | None = None) -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    limit = limit or settings.buyer_acq_reply_poll_limit
    processed = 0
    auto_replied = 0
    ignored = 0
    failures: list[str] = []

    if not settings.buyer_acq_mailbox_address or not settings.buyer_acq_mailbox_password:
        return {"status": "skipped", "summary": "missing mailbox credentials"}

    try:
        mail = imaplib.IMAP4_SSL(settings.buyer_acq_imap_host, settings.buyer_acq_imap_port)
        mail.login(settings.buyer_acq_mailbox_address, settings.buyer_acq_mailbox_password)
        mail.select("INBOX")
        typ, data = mail.search(None, "UNSEEN")
        if typ != "OK":
            mail.logout()
            return {"status": "error", "summary": "imap search failed"}

        msg_ids = data[0].split()[-limit:]
        if not msg_ids:
            mail.logout()
            return {"status": "ok", "processed": 0, "auto_replied": 0, "ignored": 0, "failures": []}

        with SessionLocal() as session:
            for msg_id in msg_ids:
                try:
                    typ, msg_data = mail.fetch(msg_id, "(RFC822)")
                    if typ != "OK":
                        failures.append(f"fetch_failed:{msg_id.decode()}")
                        continue

                    msg = email.message_from_bytes(msg_data[0][1])
                    from_name, from_email = parseaddr(outreach._decode_header_value(msg.get("From")))
                    from_email = from_email.strip().lower()
                    subject = outreach._decode_header_value(msg.get("Subject"))
                    body = outreach._extract_text_from_message(msg)

                    if not from_email:
                        ignored += 1
                        continue

                    prospect = session.execute(
                        select(AcquisitionProspect).where(AcquisitionProspect.contact_email == from_email)
                    ).scalar_one_or_none()

                    if prospect is None:
                        ignored += 1
                        continue

                    outreach._log_event(
                        session,
                        "custom_outreach_reply_seen",
                        prospect.external_id,
                        "reply seen in mailbox",
                        {
                            "from_email": from_email,
                            "from_name": from_name,
                            "subject": subject,
                            "body": body[:3000],
                        },
                    )

                    intent, auto_reply = optimized_auto_reply_text(body)

                    if intent == "negative":
                        prospect.status = "replied"
                    elif auto_reply:
                        send_result = _send_direct_smtp(
                            to_email=from_email,
                            subject=f"Re: {subject or 'Quick question'}",
                            plain_text=auto_reply,
                            html_body=auto_reply.replace("\n", "<br>"),
                        )
                        outreach._log_event(
                            session,
                            "custom_outreach_auto_reply_sent",
                            prospect.external_id,
                            f"auto replied ({intent})",
                            {
                                "subject": subject,
                                "reply_text": auto_reply,
                                "to_email": from_email,
                                "intent": intent,
                                **send_result,
                            },
                        )
                        prospect.status = "interested"
                        auto_replied += 1
                    else:
                        prospect.status = "manual_review"

                    processed += 1
                except Exception as exc:
                    failures.append(str(exc))

            session.commit()

        mail.logout()
    except Exception as exc:
        return {"status": "error", "summary": str(exc)}

    return {
        "status": "ok",
        "processed": processed,
        "auto_replied": auto_replied,
        "ignored": ignored,
        "failures": failures,
    }


def apply_relay_reply_autoclose_patch() -> None:
    global _applied, _original_outreach_status
    if _applied:
        return

    import app.api.routes.custom_outreach as outreach_route
    import app.services.autonomous_ops as ops
    import app.services.custom_outreach as outreach

    _original_outreach_status = outreach.outreach_status
    outreach.poll_reply_mailbox = optimized_poll_reply_mailbox
    outreach.outreach_status = optimized_outreach_status
    ops.outreach_status = optimized_outreach_status
    outreach_route.poll_reply_mailbox = optimized_poll_reply_mailbox
    outreach_route.outreach_status = optimized_outreach_status

    _applied = True


def optimized_outreach_status() -> dict[str, Any]:
    assert _original_outreach_status is not None
    status = _original_outreach_status()
    status["reply_autoclose_mode"] = "transactional_smtp_cap_bypass"
    return status
