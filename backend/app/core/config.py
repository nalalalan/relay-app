
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
    from_email_outbound: str = "Alan @ AO Labs <hello@mail.aolabs.io>"
    from_email_fulfillment: str = "Alan @ AO Labs <hello@mail.aolabs.io>"
    landing_page_url: str = "https://relay.aolabs.io"
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

    packet_offer_name: str = "One live packet - $40"
    packet_checkout_url: str = "https://buy.stripe.com/bJeaEZb4mf6de64dSi2Nq02"
    client_intake_destination: str = ""
    ops_admin_token: str = ""


settings = Settings()
