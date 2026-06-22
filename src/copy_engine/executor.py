"""Simulation-only trade executor — no signing, no live orders."""
import time
from loguru import logger


class TradeExecutor:
    """Always runs in simulation mode. Returns a fake order ID for every call."""

    def __init__(self, **kwargs):
        logger.info("TradeExecutor: simulation mode (no real orders)")

    async def execute_market_order(self, symbol, side, size, leverage=1, reduce_only=False):
        oid = f"sim_{symbol}_{int(time.time()*1000)}"
        logger.debug(f"SIM MARKET {side} {size} {symbol} {leverage}x → {oid}")
        return oid

    async def execute_limit_order(self, symbol, side, size, price, leverage=1, reduce_only=False, post_only=False):
        oid = f"sim_{symbol}_{int(time.time()*1000)}"
        logger.debug(f"SIM LIMIT {side} {size} {symbol} @ {price} {leverage}x → {oid}")
        return oid

    async def close_position(self, symbol, size=None, side=None):
        oid = f"sim_close_{symbol}_{int(time.time()*1000)}"
        logger.debug(f"SIM CLOSE {symbol} → {oid}")
        return oid

    async def cancel_order(self, symbol, order_id):
        return True

    async def cancel_all_orders(self, symbol=None):
        return 0
