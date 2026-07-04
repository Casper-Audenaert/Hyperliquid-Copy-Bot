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

class SizingConfig(BaseModel):
    max_position_size: float = 1000.0
    max_total_exposure: float = 5000.0

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
    copy_existing_positions: bool = True
    copy_existing_orders: bool = True
    copy_open_positions: bool = True
    auto_adjust_size: bool = True
    use_limit_orders: bool = False  # Convert market orders to limit orders at fill price
    max_open_trades: Optional[int] = None  # None = unlimited
    max_open_orders: Optional[int] = None  # None = unlimited
    max_account_equity: Optional[float] = None  # None = unlimited
    max_entry_deviation_pct: float = 5.0  # skip a copy if price has already moved this far from the target's fill
    min_position_size_usd: float = 10.0
    blocked_assets: list[str] = []  # Assets to NOT copy (e.g., ["BTC", "ETH"])

class RiskManagementConfig(BaseModel):
    max_concurrent_positions: int = 10
    max_daily_loss_usd: float = 500.0
    fast_loss_pct: float = 0.05           # pause if equity drops this fraction within the window
    fast_loss_window_secs: int = 300      # rolling window for the circuit breaker (5 minutes)
    max_net_exposure_pct: float = 0.80    # |long_notional - short_notional| / equity cap

class CopyStyleConfig(BaseModel):
    hft_threshold_fills_per_hour: int = 60   # fills/hr above this → debounced copy mode
    hft_debounce_secs: int = 30              # seconds to wait before confirming a debounced copy
    fast_burst_window_secs: int = 90          # rolling window (seconds) for the fast in-memory burst check
    fast_burst_same_symbol_closes: int = 3    # closes on ONE symbol within window → targeted churn signal
    fast_burst_total_closes: int = 6          # total closes (any symbol combined) → broad-activity fallback

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
    sizing: SizingConfig = Field(default_factory=SizingConfig)
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
        wallets_env = os.getenv('TARGET_WALLETS', '')
        settings.target_wallets = [w.strip() for w in wallets_env.split(',') if w.strip()]
        if not settings.target_wallets and settings.target_wallet:
            settings.target_wallets = [settings.target_wallet]
        labels_env = os.getenv('WALLET_LABELS', '')
        settings.wallet_labels = [l.strip() for l in labels_env.split(',') if l.strip()]

        # Wallets are managed via the GUI/DB — no wallet env var required at startup
        if not settings.target_wallet and settings.target_wallets:
            settings.target_wallet = settings.target_wallets[0]

        # Trading mode
        sim_trading = os.getenv('SIMULATED_TRADING', 'true').lower()
        settings.simulated_trading = sim_trading in ('true', '1', 'yes')
        
        sim_balance = os.getenv('SIMULATED_ACCOUNT_BALANCE', '1000.0')
        settings.simulated_account_balance = float(sim_balance)
        
        # Copy trading settings
        copy_open_pos = os.getenv('COPY_OPEN_POSITIONS', 'true').lower()
        settings.copy_rules.copy_open_positions = copy_open_pos in ('true', '1', 'yes')
        
        copy_orders = os.getenv('COPY_EXISTING_ORDERS', 'true').lower()
        settings.copy_rules.copy_existing_orders = copy_orders in ('true', '1', 'yes')
        
        auto_adjust = os.getenv('AUTO_ADJUST_SIZE', 'true').lower()
        settings.copy_rules.auto_adjust_size = auto_adjust in ('true', '1', 'yes')
        
        use_limit = os.getenv('USE_LIMIT_ORDERS', 'false').lower()
        settings.copy_rules.use_limit_orders = use_limit in ('true', '1', 'yes')
        
        # Leverage adjustment (default 1.0 = mirror target's leverage exactly;
        # this fallback must match LeverageConfig.adjustment_ratio's default
        # above, since this line unconditionally overwrites it either way)
        leverage_adj = os.getenv('LEVERAGE_ADJUSTMENT', '1.0')
        settings.leverage.adjustment_ratio = float(leverage_adj)
        
        max_trades = os.getenv('MAX_OPEN_TRADES', 'x')
        settings.copy_rules.max_open_trades = None if max_trades.lower() == 'x' else int(max_trades)
        
        max_orders = os.getenv('MAX_OPEN_ORDERS', 'x')
        settings.copy_rules.max_open_orders = None if max_orders.lower() == 'x' else int(max_orders)
        
        max_equity = os.getenv('MAX_ACCOUNT_EQUITY', 'x')
        settings.copy_rules.max_account_equity = None if max_equity.lower() == 'x' else float(max_equity)
        
        # Blocked assets
        blocked = os.getenv('BLOCKED_ASSETS', '')
        settings.copy_rules.blocked_assets = [
            asset.strip().upper() for asset in blocked.split(',') if asset.strip()
        ]
        
        settings.telegram.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        settings.telegram.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        settings.log_level = os.getenv('LOG_LEVEL', settings.log_level)
        settings.log_file = os.getenv('LOG_FILE', settings.log_file)
        settings.database_url = os.getenv('DATABASE_URL', settings.database_url)
        settings.taker_fee_rate = float(os.getenv('TAKER_FEE_RATE', str(settings.taker_fee_rate)))
        
        return settings

# Global settings instance
settings = Settings.load()
