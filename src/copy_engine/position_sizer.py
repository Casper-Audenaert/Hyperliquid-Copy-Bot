import math

from loguru import logger


class PositionSizer:
    """
    Leverage adjustment for copied positions. Position sizing itself lives in
    web/sim.py (copy_ratio, fixed at session start) — this class only adjusts
    leverage relative to the target's, with per-asset caps.
    """

    # Per-asset max leverage on Hyperliquid. NO LONGER USED TO CAP COPIES — see
    # calculate_leverage. Kept because web/sim.py's _asset_max_leverage falls back
    # to it when the live allPerpMetas fetch hasn't landed, and maintenance margin
    # (and therefore the liquidation price) genuinely does depend on the asset's
    # own maximum, not on the leverage a position happens to use.
    _MAX_LEVERAGE: dict = {
        'BTC': 50, 'ETH': 50,
        'SOL': 20, 'MATIC': 20, 'ARB': 20, 'OP': 20, 'AVAX': 20, 'DOGE': 20,
        'ATOM': 10, 'LTC': 10, 'BCH': 10, 'LINK': 10, 'UNI': 10, 'APE': 10,
        'APT': 10, 'SUI': 10, 'TIA': 10, 'SEI': 10, 'WLD': 10, 'NEAR': 10,
        'FET': 10, 'INJ': 10, 'STX': 10, 'PEPE': 10, 'BONK': 10, 'WIF': 10,
        'HYPE': 10, 'ZEC': 10, 'TRUMP': 10, 'MELANIA': 10, 'PUMP': 10,
    }

    def calculate_leverage(
        self,
        target_leverage: float,
        adjustment_ratio: float = 1.0,
        max_leverage: float = 10.0,
        min_leverage: float = 1.0,
        symbol: str = "",
    ) -> int:
        """
        Return an integer leverage mirrored from the target's leverage.
        Hyperliquid only accepts integer values.

        adjustment_ratio=1.0 (the default) mirrors the target exactly.

        NO MAXIMUM IS APPLIED. This used to clamp to
        `_MAX_LEVERAGE.get(symbol, max_leverage)`, which was wrong in both
        directions:

        - The cap is redundant. The target is a REAL Hyperliquid account, so
          whatever leverage it is running has already been accepted by the
          exchange and is by definition within that asset's real limit. Mirroring
          it can never produce an impossible position.
        - The cap was actively lossy. `_MAX_LEVERAGE` is a hand-maintained table
          of ~31 symbols; every other asset on Hyperliquid fell through to the
          10x default. A target running 25x on any unlisted asset was silently
          copied at 10x, understating both the risk and the P&L swing of the very
          position being simulated — with nothing in the UI saying so.

        `max_leverage` is still accepted so existing call sites keep working, but
        it no longer bounds the result.

        min_leverage is still enforced: leverage below 1x is not a real position.
        Non-finite/garbage input falls back to 1x — that's input validation
        against a bad parse, not a cap.
        """
        try:
            adjusted = float(target_leverage) * float(adjustment_ratio)
        except (TypeError, ValueError):
            adjusted = 0.0
        if not math.isfinite(adjusted) or adjusted <= 0:
            logger.warning(
                f"Leverage: unusable target leverage {target_leverage!r} for {symbol or '?'} — using {int(min_leverage)}x"
            )
            return max(int(min_leverage), 1)
        result = max(int(min_leverage), round(adjusted))
        logger.debug(f"Leverage: {target_leverage}x * {adjustment_ratio} -> {result}x (mirrored, uncapped)")
        return result
