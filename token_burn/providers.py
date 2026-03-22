"""Static provider metadata used by the app and settings UI."""

from __future__ import annotations

PROVIDER_SPECS = {
    "claude": {
        "provider": "claude",
        "display_name": "Claude",
        "default_collector_type": "json_api",
        "default_credential_type": "cookie_header",
        "default_usage_url": "",
        "usage_placeholder": "https://claude.ai/api/organizations/<org-id>/usage",
        "description": "Paste the exact Claude usage JSON endpoint and the Cookie header from devtools.",
        "secret_hint": "Paste the full Cookie request header from the Claude usage request.",
    },
    "codex": {
        "provider": "codex",
        "display_name": "Codex",
        "default_collector_type": "json_api",
        "default_credential_type": "cookie_header",
        "default_usage_url": "https://chatgpt.com/backend-api/wham/usage",
        "usage_placeholder": "https://chatgpt.com/backend-api/wham/usage",
        "description": "Best path: paste the full curl command from the Codex usage request in devtools so the app can import the bearer token and request headers.",
        "secret_hint": "Paste the full Cookie request header from the ChatGPT usage request.",
    },
}


def provider_choices() -> list[dict[str, str]]:
    return [PROVIDER_SPECS[key] for key in sorted(PROVIDER_SPECS)]
