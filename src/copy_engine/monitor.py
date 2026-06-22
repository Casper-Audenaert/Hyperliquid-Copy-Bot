import asyncio
import aiohttp
from typing import Callable, Optional, List
from loguru import logger
from hyperliquid.client import HyperliquidClient
from hyperliquid.websocket import HyperliquidWebSocket
from hyperliquid.models import Position, Order, UserState, WebSocketUpdate


class WalletMonitor:
    def __init__(
        self,
        target_address: str,
        api_url: str = "https://api.hyperliquid.xyz",
        ws_url: str = "wss://api.hyperliquid.xyz/ws",
    ):
        self.target_address = target_address
        self.client = HyperliquidClient(api_url)
        self.ws = HyperliquidWebSocket(ws_url)

        self.current_state: Optional[UserState] = None
        self.last_positions: List[Position] = []
        self.last_orders: List[Order] = []

        # Track the timestamp of the last processed fill for gap recovery
        # ponytail: last_fill_time=0 on startup → only replays on reconnect after first fill
        self._last_fill_time: int = 0

        # Callbacks set by the session layer
        self.on_new_position: Optional[Callable] = None
        self.on_position_update: Optional[Callable] = None
        self.on_position_close: Optional[Callable] = None
        self.on_new_order: Optional[Callable] = None
        self.on_order_fill: Optional[Callable] = None

        logger.info(f"WalletMonitor initialised for {target_address}")

    async def get_current_state(self) -> Optional[UserState]:
        async with self.client:
            self.current_state = await self.client.get_user_state(self.target_address)
            if self.current_state:
                self.last_positions = self.current_state.positions.copy()
                self.last_orders = self.current_state.orders.copy()
        return self.current_state

    async def start_monitoring(self):
        logger.info(f"Starting monitoring for {self.target_address}")
        await self.get_current_state()
        await self.ws.connect()

        # Wire up reconnect-based fill-gap recovery
        self.ws.on_reconnect = self._replay_missed_fills

        # userEvents carries fills + positions + orders in one message.
        # We do NOT subscribe to orderUpdates — it would double-trigger on_new_order (Bug 5).
        await self.ws.subscribe_user_events(self.target_address, self._handle_user_event)
        await self.ws.listen()

    async def stop_monitoring(self):
        logger.info("Stopping wallet monitoring")
        await self.ws.stop()

    # ── Main event handler ────────────────────────────────────────────────────

    async def _handle_user_event(self, update: WebSocketUpdate):
        logger.debug(f"WS event: {update.channel}")
        try:
            if "data" not in update.data:
                return
            data = update.data["data"]

            # ONE state refresh per event (was 3×, each hitting 9 dexes = 27 calls).
            # ponytail: one refresh per event; add per-symbol price cache if rate limits bite
            await self.get_current_state()

            if "fills" in data:
                await self._handle_fills(data["fills"])
            if "positions" in data:
                await self._handle_positions(data["positions"])
            # orders in userEvents are informational; new-order copying happens via fills

        except Exception as e:
            logger.error(f"Error handling WS event: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # ── Fill handling ─────────────────────────────────────────────────────────

    async def _handle_fills(self, fills: List[dict]):
        from config.settings import settings
        for fill in fills:
            symbol = fill.get("coin", "").upper()
            if symbol in settings.copy_rules.blocked_assets:
                logger.debug(f"Blocked asset fill skipped: {symbol}")
                continue

            fill_time = fill.get("time", 0)
            if fill_time > self._last_fill_time:
                self._last_fill_time = fill_time

            logger.info(f"Fill: {fill.get('dir','')} {symbol} sz={fill.get('sz')} px={fill.get('px')}")
            if self.on_order_fill:
                try:
                    if asyncio.iscoroutinefunction(self.on_order_fill):
                        await self.on_order_fill(fill)
                    else:
                        self.on_order_fill(fill)
                except Exception as e:
                    logger.error(f"Fill callback error: {e}")

    async def _replay_missed_fills(self):
        """Fetch fills newer than last_fill_time after a WS reconnect and replay them."""
        if self._last_fill_time == 0:
            return  # No fills processed yet; nothing to replay
        logger.info(f"Replaying missed fills since t={self._last_fill_time}…")
        try:
            async with aiohttp.ClientSession() as http:
                async with http.post(
                    self.client.info_url,
                    json={"type": "userFills", "user": self.target_address},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    fills = await resp.json()
            if not isinstance(fills, list):
                return
            new_fills = [f for f in fills if f.get("time", 0) > self._last_fill_time]
            if new_fills:
                logger.info(f"Replaying {len(new_fills)} missed fills")
                await self._handle_fills(sorted(new_fills, key=lambda f: f.get("time", 0)))
        except Exception as e:
            logger.warning(f"Fill replay failed: {e}")

    # ── Position handling ─────────────────────────────────────────────────────

    async def _handle_positions(self, positions: List[dict]):
        """Detect open/close/update transitions from the position snapshot.
        New positions: on_new_position is a no-op (fills are the authoritative copy signal).
        Close/update: still trigger callbacks for safety-net close handling.
        """
        from config.settings import settings
        for pos_data in positions:
            symbol = pos_data.get("coin", "").upper()
            size = float(pos_data.get("szi", 0))

            if symbol in settings.copy_rules.blocked_assets:
                continue

            existing = next((p for p in self.last_positions if p.symbol == symbol), None)

            if existing and size == 0:
                logger.info(f"Position closed (snapshot): {symbol}")
                if self.on_position_close:
                    try:
                        if asyncio.iscoroutinefunction(self.on_position_close):
                            await self.on_position_close(pos_data)
                        else:
                            self.on_position_close(pos_data)
                    except Exception as e:
                        logger.error(f"Position-close callback error: {e}")

            elif existing and abs(size) != abs(existing.size):
                # Size changed — fills already handled the copy; this is informational
                logger.debug(f"Position updated (snapshot): {symbol} {existing.size}→{size}")
                if self.on_position_update:
                    try:
                        if asyncio.iscoroutinefunction(self.on_position_update):
                            await self.on_position_update(pos_data)
                        else:
                            self.on_position_update(pos_data)
                    except Exception as e:
                        logger.error(f"Position-update callback error: {e}")
