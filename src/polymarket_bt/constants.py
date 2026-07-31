from enum import StrEnum

SCHEMA_VERSION = 1
MATCHER_VERSION = "btc-15m-multisignal-v1"
COLLECTOR_NAME = "polymarket-btc-backtester"

POLYMARKET_PRICE_SCALE = 1_000_000
SHARE_SIZE_SCALE = 1_000_000
USDC_SCALE = 1_000_000
# Chainlink RTDS values observed on 2026-07-31 carry as many as 12 decimal
# places.  A 1e12 scale preserves those public source values exactly while
# remaining safely inside int64 for any plausible BTC/USD price.
BTC_PRICE_SCALE = 1_000_000_000_000
BPS_SCALE = 1_000_000

PRICE_MIN_SCALED = 0
PRICE_MAX_SCALED = POLYMARKET_PRICE_SCALE


class Source(StrEnum):
    GAMMA = "gamma"
    CLOB_REST = "clob_rest"
    CLOB_MARKET_WS = "clob_market_ws"
    RTDS = "rtds"
    DATA_API = "data_api"
    INTERNAL = "internal"


class ReplayClockMode(StrEnum):
    EXCHANGE_TIME = "exchange_time"
    LOCAL_RECEIVE_TIME = "local_receive_time"


class QualityState(StrEnum):
    COMPLETE = "complete"
    DEGRADED = "degraded"
    UNRELIABLE = "unreliable"
    EXCLUDED = "excluded"
