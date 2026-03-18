"""Configuration management for the ingestion service."""

from dataclasses import dataclass
import os
from typing import List


def _csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    coingecko_api_base: str
    coingecko_api_key: str | None
    vs_currency: str
    coins: List[str]
    order: str
    per_page: int
    poll_interval_seconds: int
    request_timeout_seconds: int
    kafka_bootstrap_servers: List[str]
    kafka_topic_raw: str
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            coingecko_api_base=os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com/api/v3"),
            coingecko_api_key=os.getenv("COINGECKO_API_KEY"),
            vs_currency=os.getenv("COINGECKO_VS_CURRENCY", "usd"),
            coins=_csv("COINGECKO_COINS", "bitcoin,ethereum,solana"),
            order=os.getenv("COINGECKO_ORDER", "market_cap_desc"),
            per_page=_int("COINGECKO_PER_PAGE", 50),
            poll_interval_seconds=_int("POLL_INTERVAL_SECONDS", 60),
            request_timeout_seconds=_int("REQUEST_TIMEOUT_SECONDS", 15),
            kafka_bootstrap_servers=_csv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic_raw=os.getenv("KAFKA_TOPIC_RAW", "raw_crypto_data"),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )

    def coingecko_headers(self) -> dict:
        if not self.coingecko_api_key:
            return {}
        return {"x-cg-pro-api-key": self.coingecko_api_key}
