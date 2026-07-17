import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class HyperliquidConfig(BaseModel):
    api_url: str = Field(default="https://api.hyperliquid.xyz")
    ws_url: str = Field(default="wss://api.hyperliquid.xyz/ws")
    wallet_address: Optional[str] = None
    private_key: Optional[str] = None

class TelegramConfig(BaseModel):
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None
    report_interval_hours: int = 1
    api_base_url: str = "https://api.telegram.org"

class LeverageConfig(BaseModel):
    # 1.0 = mirror the target's leverage exactly (still clamped to the real
    # per-asset Hyperliquid cap below, same as it would be on a live exchange).
    # Matches how real copy-trading platforms actually behave: they either
    # mirror the leader's leverage exactly or let you pick one fixed leverage
    # for all copies -- a ratio scaling the leader's leverage isn't a mode any
    # real platform offers, so mirroring is the faithful default here.
    adjustment_ratio: float = 1.0
    max_leverage: float = 10.0
    min_leverage: float = 1.0

class CopyRulesConfig(BaseModel):
    max_open_trades: Optional[int] = None  # None = unlimited
    max_entry_deviation_pct: float = 5.0  # skip a copy if price has already moved this far from the target's fill
    min_position_size_usd: float = 10.0   # dust floor — HL's real minimum order notional; fills below this are skipped
    blocked_assets: list[str] = []  # Assets to NOT copy (e.g., ["BTC", "ETH"])

class RiskManagementConfig(BaseModel):
    max_daily_loss_usd: float = 500.0
    fast_loss_pct: float = 0.05           # pause if equity drops this fraction within the window
    fast_loss_window_secs: int = 300      # rolling window for the circuit breaker (5 minutes)

class CopyStyleConfig(BaseModel):
    hft_threshold_fills_per_hour: int = 60   # fills/hr above this → "HFT" badge (informational only; all styles copy every fill live)

class SimAccuracyConfig(BaseModel):
    slippage_bps: float = 3.0          # basis points of slippage per side on every fill
    sim_latency_ms: int = 150          # ms of execution delay (price drift approximation)
    maker_close_rate: float = 0.0      # fraction of close fills charged at maker fee rate

class StartupSeedingPolicy(BaseModel):
    # always_skip = "new trades only", matching how Bybit/Binance/OKX copy trading
    # works: a target's pre-existing positions are never seeded — they become
    # ghosts, and each symbol unblocks for copying once the target closes it.
    # (eToro-style "copy open trades" = always_seed + startup_seed_size_multiplier 1.0)
    startup_mode: str = "always_skip"             # "smart_safe" | "always_seed" | "always_skip"
    max_seed_drift_pct: float = 0.015             # 1.5% max entry drift before ghosting
    max_seed_leverage: int = 4                    # cap leverage at seed time
    startup_seed_size_multiplier: float = 0.35    # scale down seed size vs live copy size
    max_seed_position_notional_pct: float = 0.03  # 3% per-position exposure limit
    max_total_copied_exposure_pct: float = 0.25   # 25% total portfolio exposure limit
    max_symbol_exposure_pct: float = 0.10         # 10% per-symbol exposure limit
    pause_on_daily_loss_pct: float = 0.03         # skip seeding if today's loss >= 3%
    pause_on_total_drawdown_pct: float = 0.10     # skip seeding if drawdown >= 10%
    allow_seed_small: bool = True                 # use SEED_SMALL for borderline positions
    ghost_on_failed_seed: bool = True             # ghost instead of ignoring failed seed orders

def _parse_csv_env(raw: str) -> list[str]:
    """Parse a comma-separated env var into a stripped, non-empty item list.

    BUG FIX: python-dotenv only strips a trailing `# comment` when the value
    before it is non-empty (e.g. `KEY=x  # comment` -> "x"); for an EMPTY
    value followed by a comment (`KEY=  # comment`), it returns the comment
    text itself as the literal value instead of "". That silently turned
    `BLOCKED_ASSETS=  # Comma-separated list of assets to NOT copy (e.g.,
    BTC,ETH,SOL)` into blocked_assets=["...NOT COPY (E.G.", "BTC", "ETH",
    "SOL)"] — every BTC and ETH fill for every wallet was silently treated
    as a blocked asset and never copied. Stripping any `#...` suffix here
    (in addition to fixing the .env file itself) makes every comma-list env
    var immune to this whole class of misconfiguration, not just this one
    instance of it.
    """
    value = raw.split("#", 1)[0]
    return [item.strip() for item in value.split(",") if item.strip()]


def _validate_eth_address(v: Optional[str], field_name: str) -> Optional[str]:
    """Validate Ethereum address format."""
    if not v:
        return v
    if not (len(v) == 42 and v.startswith("0x") and all(c in "0123456789abcdefABCDEF" for c in v[2:])):
        raise ValueError(f"{field_name} must be a valid 0x Ethereum address (42 chars), got: {v!r}")
    return v.lower()


class Settings(BaseModel):
    # Target address to copy (wallet or vault - the bot treats them the same)
    # Must be set via TARGET_WALLET_ADDRESS env var — no default to prevent accidental live trading
    target_wallet: str = ""
    # Multi-wallet support (web dashboard): comma-separated addresses via TARGET_WALLETS env var
    target_wallets: list[str] = []
    wallet_labels: list[str] = []

    # Trading mode
    simulated_trading: bool = True
    simulated_account_balance: float = 1000.0

    # Configuration sections
    hyperliquid: HyperliquidConfig = Field(default_factory=HyperliquidConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    leverage: LeverageConfig = Field(default_factory=LeverageConfig)
    copy_rules: CopyRulesConfig = Field(default_factory=CopyRulesConfig)
    risk_management: RiskManagementConfig = Field(default_factory=RiskManagementConfig)
    seed_policy: StartupSeedingPolicy = Field(default_factory=StartupSeedingPolicy)
    copy_style: CopyStyleConfig = Field(default_factory=CopyStyleConfig)
    sim_accuracy: SimAccuracyConfig = Field(default_factory=SimAccuracyConfig)

    # Paths
    log_level: str = "INFO"
    log_file: str = "./logs/trading.log"
    database_url: str = "sqlite:///./data/trading.db"
    taker_fee_rate: float = 0.00045  # HL base taker fee Tier 0 (4.5 bps); Tier 2 = 0.00035; override via TAKER_FEE_RATE env

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

    @field_validator("simulated_account_balance", mode="before")
    @classmethod
    def validate_balance(cls, v: float) -> float:
        if float(v) <= 0:
            raise ValueError("simulated_account_balance must be positive")
        return v

    @model_validator(mode="after")
    def validate_addresses(self) -> "Settings":
        _validate_eth_address(self.target_wallet, "target_wallet")
        _validate_eth_address(self.hyperliquid.wallet_address, "wallet_address")
        if (
            self.target_wallet
            and self.hyperliquid.wallet_address
            and self.target_wallet.lower() == self.hyperliquid.wallet_address.lower()
        ):
            raise ValueError("target_wallet and wallet_address must be different accounts")
        return self

    @classmethod
    def load(cls) -> 'Settings':
        """Load settings from environment variables"""
        settings = cls()
        
        # Load from environment
        settings.hyperliquid.api_url = os.getenv('HYPERLIQUID_API_URL', settings.hyperliquid.api_url)
        settings.hyperliquid.wallet_address = os.getenv('HYPERLIQUID_WALLET_ADDRESS')
        settings.hyperliquid.private_key = os.getenv('HYPERLIQUID_PRIVATE_KEY')
        
        settings.target_wallet = os.getenv('TARGET_WALLET_ADDRESS', settings.target_wallet)

        # Multi-wallet support (web dashboard)
        settings.target_wallets = _parse_csv_env(os.getenv('TARGET_WALLETS', ''))
        if not settings.target_wallets and settings.target_wallet:
            settings.target_wallets = [settings.target_wallet]
        settings.wallet_labels = _parse_csv_env(os.getenv('WALLET_LABELS', ''))

        # Wallets are managed via the GUI/DB — no wallet env var required at startup
        if not settings.target_wallet and settings.target_wallets:
            settings.target_wallet = settings.target_wallets[0]

        # Trading mode
        sim_trading = os.getenv('SIMULATED_TRADING', 'true').lower()
        settings.simulated_trading = sim_trading in ('true', '1', 'yes')
        
        sim_balance = os.getenv('SIMULATED_ACCOUNT_BALANCE', '1000.0')
        settings.simulated_account_balance = float(sim_balance)
        
        # Leverage adjustment (default 1.0 = mirror target's leverage exactly;
        # this fallback must match LeverageConfig.adjustment_ratio's default
        # above, since this line unconditionally overwrites it either way)
        leverage_adj = os.getenv('LEVERAGE_ADJUSTMENT', '1.0')
        settings.leverage.adjustment_ratio = float(leverage_adj)
        
        max_trades = os.getenv('MAX_OPEN_TRADES', 'x')
        settings.copy_rules.max_open_trades = None if max_trades.lower() == 'x' else int(max_trades)

        settings.copy_rules.min_position_size_usd = float(
            os.getenv('MIN_POSITION_SIZE_USD', str(settings.copy_rules.min_position_size_usd))
        )

        # Blocked assets
        settings.copy_rules.blocked_assets = [
            asset.upper() for asset in _parse_csv_env(os.getenv('BLOCKED_ASSETS', ''))
        ]
        
        settings.telegram.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        settings.telegram.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        settings.telegram.api_base_url = os.getenv('TELEGRAM_API_BASE_URL', settings.telegram.api_base_url)
        
        settings.log_level = os.getenv('LOG_LEVEL', settings.log_level)
        settings.log_file = os.getenv('LOG_FILE', settings.log_file)
        settings.database_url = os.getenv('DATABASE_URL', settings.database_url)
        settings.taker_fee_rate = float(os.getenv('TAKER_FEE_RATE', str(settings.taker_fee_rate)))
        
        return settings

# Global settings instance
settings = Settings.load()
