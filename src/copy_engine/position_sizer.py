from loguru import logger


class PositionSizer:
    """
    Leverage adjustment for copied positions. Position sizing itself lives in
    web/sim.py (copy_ratio, fixed at session start) — this class only adjusts
    leverage relative to the target's, with per-asset caps.
    """

    # Per-asset max leverage limits on Hyperliquid (default 10x for unknowns)
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
        Return an integer leverage adjusted from the target's leverage.
        Hyperliquid only accepts integer values; per-asset caps are enforced.
        adjustment_ratio=1.0 (the default) mirrors the target's leverage
        exactly, subject to those caps -- every call site in this codebase
        passes settings.leverage.adjustment_ratio explicitly, so this default
        only matters as documentation / a safe fallback for any future caller.
        """
        asset_cap = self._MAX_LEVERAGE.get(symbol.upper(), int(max_leverage))
        adjusted = target_leverage * adjustment_ratio
        result = max(int(min_leverage), min(round(adjusted), asset_cap))
        logger.debug(f"Leverage: {target_leverage}x * {adjustment_ratio} -> {result}x (cap {asset_cap}x)")
        return result
