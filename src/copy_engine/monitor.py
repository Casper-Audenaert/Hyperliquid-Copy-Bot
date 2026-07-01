import asyncio
import aiohttp
from typing import Callable, Optional, List
from loguru import logger
from hyperliquid.client import HyperliquidClient, get_startup_sem
from hyperliquid.websocket import HyperliquidWebSocket
from hyperliquid.models import Position, Order, UserState, WebSocketUpdate, PositionSide


# ── Shared WebSocket pool ─────────────────────────────────────────────────────
# HL enforces 15 WS connections per IP.  All monitors share connections from
# this pool rather than each opening their own.  Each WS handles unlimited
# subscriptions, so in practice a single connection serves all wallets.
_MAX_WS_CONNECTIONS = 14     # keep 1 slot free for other HL WS usage
_ws_pool: List[HyperliquidWebSocket] = []
_ws_pool_lock: Optional[asyncio.Lock] = None   # lazily created in the asyncio loop


def _get_pool_lock() -> asyncio.Lock:
    global _ws_pool_lock
    if _ws_pool_lock is None:
        _ws_pool_lock = asyncio.Lock()
    return _ws_pool_lock


async def _acquire_shared_ws(ws_url: str) -> HyperliquidWebSocket:
    """Return the WS connection with fewest subscriptions, or create a new one if
    the pool has space.  Never creates more than _MAX_WS_CONNECTIONS connections."""
    async with _get_pool_lock():
        # Prefer an existing connected WS (fewest subscribers = most balanced)
        if _ws_pool:
            best = min(_ws_pool, key=lambda w: len(w.subscriptions))
            return best

        # Pool is empty — create the first shared connection
        ws = HyperliquidWebSocket(ws_url)
        _ws_pool.append(ws)
        return ws


async def _release_wallet_from_pool(address: str, dexs: list):
    """Remove a wallet's subscriptions and callback from its shared WS.
    Called when a wallet is removed from monitoring."""
    addr_lower = address.lower()
    async with _get_pool_lock():
        for ws in _ws_pool:
            if f"user:{addr_lower}" in ws.callbacks:
                await ws.unsubscribe_user_events(address, dexs)
                await ws.unsubscribe_user_fills(address)
                # Remove the reconnect handler registered for this address
                ws.on_reconnect_handlers = [
                    h for h in ws.on_reconnect_handlers
                    if getattr(h, "__self__", None) is None
                    or getattr(h.__self__, "target_address", "").lower() != addr_lower
                ]
                break


class WalletMonitor:
    def __init__(
        self,
        target_address: str,
        api_url: str = "https://api.hyperliquid.xyz",
        ws_url: str = "wss://api.hyperliquid.xyz/ws",
    ):
        self.target_address = target_address
        self.api_url = api_url
        self.ws_url = ws_url
        self.client = HyperliquidClient(api_url)
        # ws is set to the shared connection in start_monitoring()
        self.ws: Optional[HyperliquidWebSocket] = None

        self.current_state: Optional[UserState] = None
        self.last_positions: List[Position] = []
        self.last_orders: List[Order] = []

        # Track the timestamp of the last processed fill for gap recovery
        self._last_fill_time: int = 0

        # Callbacks set by the session layer
        self.on_new_position: Optional[Callable] = None
        self.on_position_update: Optional[Callable] = None
        self.on_position_close: Optional[Callable] = None
        self.on_new_order: Optional[Callable] = None
        self.on_order_fill: Optional[Callable] = None
        self.on_leverage_change: Optional[Callable] = None  # (symbol, old_lev, new_lev)
        self.on_alert: Optional[Callable] = None  # (message: str) -> None, operator-facing warnings

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

        # Attach to (or create) a shared WS connection from the pool
        shared_ws = await _acquire_shared_ws(self.ws_url)
        self.ws = shared_ws

        # Register this monitor's reconnect/fill-replay handler
        shared_ws.on_reconnect_handlers.append(self._replay_missed_fills)

        # Serialize the is_running check + connect + listen() creation with the pool
        # lock so that when many wallets are added simultaneously, only ONE listen()
        # task is ever created per WS object.  Without this, all coroutines yield
        # inside `await shared_ws.connect()` before is_running is set to True, so
        # each one proceeds to create_task(listen()) → multiple listen() tasks on
        # the same WS → "cannot call recv while another coroutine is already waiting".
        async with _get_pool_lock():
            if not shared_ws.is_running:
                await shared_ws.connect()
                asyncio.create_task(shared_ws.listen())
                logger.info(
                    f"Started shared WS (pool size={len(_ws_pool)}) "
                    f"for {self.target_address[:10]}…"
                )
            else:
                logger.info(
                    f"Reusing shared WS (pool size={len(_ws_pool)}, "
                    f"subs={len(shared_ws.subscriptions)}) "
                    f"for {self.target_address[:10]}…"
                )

        # Background state refresh — decoupled from fill events so high-frequency
        # fills don't trigger REST calls on every message.
        asyncio.create_task(self._periodic_state_refresh())

        # Subscribe this wallet's address to the shared WS
        await shared_ws.subscribe_user_events(
            self.target_address, self.client.dexs, self._handle_user_event
        )
        # userFills backstops _replay_missed_fills: HL resends an isSnapshot=true
        # fills list on every (re)subscribe (including reconnects), so this arrives
        # for free without an extra per-DEX REST burst. Routed through the same
        # _handle_fills() path, so tid-based dedup in sim.py makes any overlap with
        # the REST replay safe.
        await shared_ws.subscribe_user_fills(self.target_address, self._handle_user_fills_event)
        # Note: do NOT call await shared_ws.listen() here — it is already running
        # as an asyncio task created above (or by a previous monitor).

    async def stop_monitoring(self):
        logger.info(f"Stopping monitoring for {self.target_address}")
        await _release_wallet_from_pool(self.target_address, self.client.dexs)

    async def _periodic_state_refresh(self):
        """Refresh current_state every ~60s independent of fill events."""
        import random
        await asyncio.sleep(random.uniform(0, 30))  # initial stagger
        while True:
            await asyncio.sleep(50 + random.uniform(0, 20))  # 50–70s jitter
            try:
                await self.get_current_state()
                logger.debug(f"State refreshed for {self.target_address[:10]}…")
            except Exception as e:
                logger.warning(f"Periodic state refresh failed: {e}")

    # ── Main event handler ────────────────────────────────────────────────────

    async def _handle_user_event(self, update: WebSocketUpdate):
        logger.debug(f"WS event: {update.channel}")
        try:
            if "data" not in update.data:
                return
            data = update.data["data"]

            # Belt-and-suspenders address check: with a shared WS connection,
            # the broadcast path may deliver another wallet's events here.
            # Reject the entire message if the outer user field doesn't match.
            event_user = (data.get("user") or "").lower()
            if event_user and event_user != self.target_address.lower():
                logger.debug(
                    f"Skipping event for {event_user[:10]}… "
                    f"(this monitor is {self.target_address[:10]}…)"
                )
                return

            if "fills" in data:
                await self._handle_fills(data["fills"])
            if "positions" in data:
                await self._handle_positions(data["positions"])

        except Exception as e:
            logger.error(f"Error handling WS event: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # ── Fill handling ─────────────────────────────────────────────────────────

    async def _handle_user_fills_event(self, update: WebSocketUpdate):
        """Handle a userFills WS message (fills-only feed, separate from userEvents).
        Reuses _handle_fills so address filtering and the on_order_fill dispatch
        (and its downstream tid dedup) stay in one place."""
        try:
            inner = update.data.get("data", {})
            fills = inner.get("fills", [])
            if fills:
                await self._handle_fills(fills)
        except Exception as e:
            logger.error(f"Error handling userFills WS event: {e}")

    async def _handle_fills(self, fills: List[dict]):
        from config.settings import settings
        for fill in fills:
            # Per-fill address check: HL includes "user" in each fill dict.
            # If it's present and doesn't match our target, skip this fill — it
            # arrived via a shared WS connection meant for a different wallet.
            fill_user = (fill.get("user") or "").lower()
            if fill_user and fill_user != self.target_address.lower():
                continue

            symbol = fill.get("coin", "").upper()
            if symbol in settings.copy_rules.blocked_assets:
                logger.debug(f"Blocked asset fill skipped: {symbol}")
                continue

            fill_time = fill.get("time", 0)
            if fill_time > self._last_fill_time:
                self._last_fill_time = fill_time

            logger.info(f"Fill: {fill.get('dir','')} {symbol} sz={fill.get('sz')} px={fill.get('px')}")
            if self.on_order_fill:
                if asyncio.iscoroutinefunction(self.on_order_fill):
                    # Dispatch as a task so slow callbacks (DB write + REST fetch) don't
                    # block subsequent fills.  session._state_lock inside _process_fill
                    # serializes per-session mutations so concurrency is safe.
                    _t = asyncio.create_task(self.on_order_fill(fill))
                    _t.add_done_callback(
                        lambda t: logger.error(f"Fill callback error: {t.exception()!r}")
                        if not t.cancelled() and t.exception() else None
                    )
                else:
                    try:
                        self.on_order_fill(fill)
                    except Exception as e:
                        logger.error(f"Fill callback error: {e}")

    async def _replay_missed_fills(self):
        """Fetch fills newer than last_fill_time after a WS reconnect and replay them."""
        if self._last_fill_time == 0:
            return
        logger.info(f"Replaying missed fills since t={self._last_fill_time}…")
        all_new: list = []
        seen_tids: set = set()
        try:
            async with aiohttp.ClientSession() as http:
                for dex in self.client.dexs:
                    try:
                        payload = {"type": "userFills", "user": self.target_address}
                        if dex:
                            payload["dex"] = dex
                        async with get_startup_sem():
                            async with http.post(
                                self.client.info_url,
                                json=payload,
                                timeout=aiohttp.ClientTimeout(total=15),
                            ) as resp:
                                fills = await resp.json()
                        if not isinstance(fills, list):
                            continue
                        for f in fills:
                            if f.get("time", 0) < self._last_fill_time:
                                continue
                            tid = f.get("tid")
                            if tid and tid in seen_tids:
                                continue
                            if tid:
                                seen_tids.add(tid)
                            all_new.append(f)
                    except Exception:
                        continue
            if all_new:
                if len(all_new) > 500:
                    dropped = len(all_new) - 500
                    logger.warning(f"Replay capped at 500 fills (had {len(all_new)}) — likely HFT target")
                    if self.on_alert:
                        self.on_alert(
                            f"Replay truncated for {self.target_address[:10]}…: "
                            f"{dropped} older missed fill(s) were dropped, not copied"
                        )
                    all_new = sorted(all_new, key=lambda f: f.get("time", 0))[-500:]
                logger.info(f"Replaying {len(all_new)} missed fills across {len(self.client.dexs)} DEX(es)")
                await self._handle_fills(sorted(all_new, key=lambda f: f.get("time", 0)))
        except Exception as e:
            logger.warning(f"Fill replay failed: {e}")

    # ── Position handling ─────────────────────────────────────────────────────

    async def _handle_positions(self, positions: List[dict]):
        """Detect open/close/update transitions and patch current_state from WS data."""
        from config.settings import settings
        for pos_data in positions:
            symbol = pos_data.get("coin", "").upper()
            size   = float(pos_data.get("szi", 0))

            if symbol in settings.copy_rules.blocked_assets:
                continue

            lev_raw  = pos_data.get("leverage", {})
            leverage = float(lev_raw.get("value", 1) if isinstance(lev_raw, dict) else lev_raw or 1)
            entry_px = float(pos_data.get("entryPx") or 0)
            pos_val  = float(pos_data.get("positionValue") or 0)
            curr_px  = (pos_val / abs(size)) if size != 0 else entry_px
            upnl     = float(pos_data.get("unrealizedPnl") or 0)
            liq_raw  = pos_data.get("liquidationPx")
            liq_px   = float(liq_raw) if liq_raw else None
            margin   = float(pos_data.get("marginUsed") or 0)

            if self.current_state is not None:
                side = PositionSide.LONG if size > 0 else PositionSide.SHORT
                if size != 0 and entry_px > 0:
                    updated      = False
                    old_leverage = 0.0
                    for i, p in enumerate(self.current_state.positions):
                        if p.symbol == symbol:
                            old_leverage = p.leverage
                            self.current_state.positions[i] = Position(
                                symbol=symbol, side=side, size=abs(size),
                                entry_price=entry_px, current_price=curr_px,
                                leverage=leverage, unrealized_pnl=upnl,
                                liquidation_price=liq_px, margin=margin,
                            )
                            updated = True
                            break
                    if not updated:
                        self.current_state.positions.append(Position(
                            symbol=symbol, side=side, size=abs(size),
                            entry_price=entry_px, current_price=curr_px,
                            leverage=leverage, unrealized_pnl=upnl,
                            liquidation_price=liq_px, margin=margin,
                        ))
                    if (updated and self.on_leverage_change
                            and old_leverage > 0 and abs(leverage - old_leverage) > 0.01):
                        try:
                            asyncio.create_task(
                                self.on_leverage_change(symbol, old_leverage, leverage)
                            )
                        except Exception:
                            pass
                elif size == 0:
                    self.current_state.positions = [
                        p for p in self.current_state.positions if p.symbol != symbol
                    ]

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
                logger.debug(f"Position updated (snapshot): {symbol} {existing.size}→{size}")
                if self.on_position_update:
                    try:
                        if asyncio.iscoroutinefunction(self.on_position_update):
                            await self.on_position_update(pos_data)
                        else:
                            self.on_position_update(pos_data)
                    except Exception as e:
                        logger.error(f"Position-update callback error: {e}")
