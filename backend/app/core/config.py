
import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    app_base_url: str = "http://localhost:8000"
    database_url: str

    openai_api_key: str = ""
    apollo_api_key: str = ""
    smartlead_api_key: str = ""
    resend_api_key: str = ""
    apify_api_token: str = ""

    cf_api_token: str = ""
    cf_zone_id: str = ""
    namecheap_api_user: str = ""
    namecheap_api_key: str = ""
    namecheap_username: str = ""

    stripe_secret_key: str = ""
    stripe_webhook_secret: str = ""
    tally_webhook_secret: str = ""

    outbound_domain: str = "mail.aolabs.io"
    fulfillment_domain: str = "mail.aolabs.io"
    from_email_outbound: str = "RelayBrief <hello@mail.aolabs.io>"
    from_email_fulfillment: str = "RelayBrief <hello@mail.aolabs.io>"
    landing_page_url: str = "https://relaybrief.com"
    reply_to_email: str = "alan@aolabs.io"

    smartlead_campaign_id: str = ""
    smartlead_webhook_secret: str = ""
    resend_webhook_secret: str = ""
    default_country: str = "US"

    apify_google_maps_actor_id: str = ""
    apify_website_crawler_actor_id: str = ""
    buyer_acq_mailbox_address: str = "alan@aolabs.io"
    buyer_acq_mailbox_password: str = ""
    buyer_acq_smtp_host: str = "smtp.porkbun.com"
    buyer_acq_smtp_port: int = 587
    buyer_acq_imap_host: str = "imap.porkbun.com"
    buyer_acq_imap_port: int = 993
    buyer_acq_daily_send_cap: int = 20
    buyer_acq_reply_poll_limit: int = 20

    acq_auto_send: bool = False
    acq_min_fit_score: int = 70
    acq_daily_search_limit: int = 25
    acq_target_person_titles: str = "Founder,Co-Founder,Owner,Managing Partner,CEO"
    acq_target_keywords: str = "paid media,ppc,google ads,meta ads,performance marketing"
    acq_excluded_keywords: str = "saas,software,transcription,call recording,note taking"

    packet_offer_name: str = "RelayBrief follow-up email"
    packet_checkout_url: str = "https://buy.stripe.com/bJe28t7Sa7DLgec29A2Nq08"
    first_money_offer_name: str = "RelayBrief $1 follow-up email"
    first_money_checkout_url: str = "https://buy.stripe.com/bJe28t7Sa7DLgec29A2Nq08"
    first_money_price_usd: float = 1.0
    minimum_weekly_target_usd: float = 1.0
    packet_5_pack_url: str = ""
    weekly_sprint_url: str = ""
    monthly_autopilot_url: str = ""
    client_intake_destination: str = "https://relaybrief.com/client-intake.html"
    ops_admin_token: str = ""


settings = Settings()


_FALSE_ENV_VALUES = {"0", "false", "no", "off", "disabled"}


def relay_costs_paused() -> bool:
    raw = os.getenv("AO_RELAY_COSTS_PAUSED", "").strip()
    if not raw:
        raw = os.getenv("AO_RELAY_MONEY_LOOP_PAUSED", "true").strip()
    return raw.lower() not in _FALSE_ENV_VALUES


def relay_paid_fulfillment_allowed_when_paused() -> bool:
    raw = os.getenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", "true").strip()
    return raw.lower() not in _FALSE_ENV_VALUES


def relay_paused_response(action: str) -> dict[str, object]:
    return {
        "status": "paused",
        "reason": "paused_by_owner_cost_control",
        "action": action,
        "summary": (
            "Relay paid/API automation is paused. Set AO_RELAY_COSTS_PAUSED=false "
            "only when deliberately re-enabling outbound and provider calls."
        ),
    }


def first_money_url_configured() -> bool:
    return bool(
        os.getenv("RELAY_FIRST_MONEY_CHECKOUT_URL", "").strip()
        or os.getenv("FIRST_MONEY_CHECKOUT_URL", "").strip()
        or str(settings.first_money_checkout_url or "").strip()
    )


def entry_checkout_url() -> str:
    return (
        os.getenv("RELAY_FIRST_MONEY_CHECKOUT_URL", "").strip()
        or os.getenv("FIRST_MONEY_CHECKOUT_URL", "").strip()
        or str(settings.first_money_checkout_url or "").strip()
        or str(settings.packet_checkout_url or "").strip()
    )


def entry_price_label() -> str:
    price = entry_price_usd()
    if price > 0:
        return f"${price:.0f}" if price.is_integer() else f"${price:.2f}"
    return "paid"


def entry_price_usd() -> float:
    if first_money_url_configured():
        raw_price = (
            os.getenv("RELAY_FIRST_MONEY_PRICE_USD", "").strip()
            or os.getenv("FIRST_MONEY_PRICE_USD", "").strip()
            or str(settings.first_money_price_usd or "").strip()
        )
        try:
            return float(raw_price)
        except Exception:
            return 0.0
    try:
        return float(os.getenv("RELAY_PACKET_PRICE_USD", "").strip() or 40)
    except Exception:
        return 40.0


def entry_offer_name() -> str:
    if first_money_url_configured():
        return (
            os.getenv("RELAY_FIRST_MONEY_OFFER_NAME", "").strip()
            or os.getenv("FIRST_MONEY_OFFER_NAME", "").strip()
            or str(settings.first_money_offer_name or "").strip()
            or "RelayBrief $1 follow-up email"
        )
    return str(settings.packet_offer_name or "RelayBrief follow-up email")
