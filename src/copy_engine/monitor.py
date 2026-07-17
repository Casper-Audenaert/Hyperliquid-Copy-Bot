import asyncio
import aiohttp
import time
from typing import Callable, Optional, List
from loguru import logger
from hyperliquid.client import HyperliquidClient, get_startup_sem
from hyperliquid.websocket import HyperliquidWebSocket
from hyperliquid.models import Position, Order, UserState, WebSocketUpdate, PositionSide


# ── Shared WebSocket pool ─────────────────────────────────────────────────────
# HL enforces 15 WS connections per IP.  All monitors share connections from
# this pool rather than each opening their own.  Each WS handles unlimited
# subscriptions, so in practice a single connection serves all wallets.
MAX_WALLETS = 14     # keep 1 slot free for other HL WS usage
_REPLAY_FILL_CAP = 2000     # Hyperliquid userFills REST endpoint's own max return size
_ws_pool: List[HyperliquidWebSocket] = []
_ws_pool_lock: Optional[asyncio.Lock] = None   # lazily created in the asyncio loop


def _get_pool_lock() -> asyncio.Lock:
    global _ws_pool_lock
    if _ws_pool_lock is None:
        _ws_pool_lock = asyncio.Lock()
    return _ws_pool_lock


def _wallets_on(ws: HyperliquidWebSocket) -> int:
    """Count distinct wallets assigned to this connection: those with a
    registered 'user:' callback PLUS those reserved at acquire time but not
    yet subscribed (subscribe_user_events runs after awaited REST calls, so
    counting callbacks alone races concurrent wallet additions)."""
    registered = {k.split(":", 1)[1] for k in ws.callbacks if k.startswith("user:")}
    return len(registered | ws.reserved_users)


async def _acquire_shared_ws(ws_url: str, address: str) -> HyperliquidWebSocket:
    """Distribute wallets one-per-connection up to MAX_WALLETS, then
    fall back to the least-loaded connection once the pool is full.

    The wallet's address is RESERVED on the chosen connection here, inside the
    pool lock, so a concurrently-added wallet can never pick the same
    connection while this one is still working through its pre-subscribe
    awaits. Guarantees one-wallet-per-connection whenever the pool has room.
    """
    addr_lower = address.lower()
    async with _get_pool_lock():
        if _ws_pool:
            free = [w for w in _ws_pool if _wallets_on(w) == 0]
            if free:
                free[0].reserved_users.add(addr_lower)
                return free[0]
            if len(_ws_pool) < MAX_WALLETS:
                ws = HyperliquidWebSocket(ws_url)
                ws.reserved_users.add(addr_lower)
                _ws_pool.append(ws)
                return ws
            ws = min(_ws_pool, key=_wallets_on)
            ws.reserved_users.add(addr_lower)
            logger.warning(
                f"WS pool full ({len(_ws_pool)} connections) — {addr_lower[:10]}… "
                f"SHARES a connection ({_wallets_on(ws)} wallets on it). "
                f"userEvents position-close/leverage signals are degraded for these "
                f"wallets; the periodic REST refresh is the fallback."
            )
            _alert_shared_connection(addr_lower, _wallets_on(ws))
            return ws

        # Pool is empty — create the first shared connection
        ws = HyperliquidWebSocket(ws_url)
        ws.reserved_users.add(addr_lower)
        _ws_pool.append(ws)
        return ws


def _alert_shared_connection(addr_lower: str, wallet_count: int):
    """Operator-facing alert when a connection ends up serving 2+ wallets —
    should never happen at ≤14 wallets now that acquisition reserves slots."""
    try:
        from web_app import _send_telegram  # late import to avoid circular
        _send_telegram(
            f"⚠️ <b>WS CONNECTION SHARING</b>\n{addr_lower[:10]}… now shares a "
            f"connection with {wallet_count - 1} other wallet(s). Position-close/"
            f"leverage WS signals degraded; REST refresh is covering."
        )
    except Exception:
        pass


async def _release_wallet_from_pool(address: str, dexs: list):
    """Remove a wallet's subscriptions, callback, and reservation from its
    shared WS. Called when a wallet is removed from monitoring."""
    addr_lower = address.lower()
    async with _get_pool_lock():
        for ws in _ws_pool:
            # Match by reservation too: a wallet that failed between acquire
            # and subscribe has a reservation but no callback — without this
            # its slot would leak and block a future wallet from a free
            # connection.
            if f"user:{addr_lower}" in ws.callbacks or addr_lower in ws.reserved_users:
                if f"user:{addr_lower}" in ws.callbacks:
                    await ws.unsubscribe_user_events(address, dexs)
                    await ws.unsubscribe_user_fills(address)
                ws.reserved_users.discard(addr_lower)
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
        last_fill_time_ms: int = 0,
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
        # Wall-clock time of the last WS message (any channel) for this wallet —
        # distinguishes "target is just idle" from "the feed died" (a connection
        # can report is_running=True while silently receiving nothing, e.g. a
        # half-open TCP connection). Surfaced as feed_age_secs on the dashboard.
        self.last_ws_event_ts: float = 0.0
        self._state_refresh_task: Optional[asyncio.Task] = None
        # Per-wallet FIFO fill queue + single consumer task (started in
        # start_monitoring). Replaces a create_task-per-fill dispatch that gave
        # no ordering guarantee — concurrent fills for the same wallet could
        # race each other through on_order_fill. A single consumer processes
        # fills strictly in arrival order within this wallet's stream (rule:
        # never debounce execution, always preserve per-wallet/per-symbol
        # order), while different wallets remain fully parallel since each
        # has its own queue/task.
        self._fill_queue: asyncio.Queue = asyncio.Queue()
        self._fill_consumer_task: Optional[asyncio.Task] = None

        # Track the timestamp of the last processed fill for gap recovery.
        # BUG FIX: this used to always start at 0 and was never persisted, so a
        # process restart made the next userFills snapshot treat EVERY fill
        # during the downtime window as pre-existing "history" and silently
        # drop all of it (see _handle_user_fills_event) — trades that actually
        # happened while the bot was down were never copied. Restoring the
        # persisted watermark here (see db_update_last_fill_time /
        # WalletSession.last_fill_time_ms) makes the same downtime window a
        # replayable gap instead.
        self._last_fill_time: int = last_fill_time_ms
        self._last_fill_persist_ts: float = 0.0  # monotonic time of last DB persist (throttle)

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

        # Attach to (or create) a shared WS connection from the pool.
        # The pool reserves this wallet's slot on the chosen connection
        # atomically, so concurrent wallet additions can't share a socket.
        shared_ws = await _acquire_shared_ws(self.ws_url, self.target_address)
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
        self._state_refresh_task = asyncio.create_task(self._periodic_state_refresh())
        self._fill_consumer_task = asyncio.create_task(self._consume_fills())

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

        # A restored (nonzero) fill clock means this process just (re)started
        # with a persisted watermark from before — replay whatever happened
        # during the downtime gap. The userFills isSnapshot delivered by the
        # subscribe above already covers HL's own "recent fills" window; this
        # REST-paginated replay is the thorough backstop for a longer gap.
        # tid dedup downstream makes any overlap between the two safe.
        if self._last_fill_time > 0:
            asyncio.create_task(self._replay_missed_fills())

    async def stop_monitoring(self):
        logger.info(f"Stopping monitoring for {self.target_address}")
        await _release_wallet_from_pool(self.target_address, self.client.dexs)
        # _periodic_state_refresh is an unconditional `while True:` loop with no
        # other exit path — without this it outlives the wallet forever, issuing
        # a REST get_user_state every 50-70s indefinitely. Over a 24/7 deployment
        # with repeated add/remove churn this accumulates one orphan loop per
        # removed wallet.
        if self._state_refresh_task is not None:
            self._state_refresh_task.cancel()
            self._state_refresh_task = None
        if self._fill_consumer_task is not None:
            self._fill_consumer_task.cancel()
            self._fill_consumer_task = None

    async def _periodic_state_refresh(self):
        """Refresh current_state every ~60s independent of fill events.

        Also the REST-driven safety net for position closes: once a wallet's
        connection is shared with others (pool full, MAX_WALLETS
        reached), unattributable userEvents messages are dropped by design
        (see websocket.py), so on_position_close never fires from WS for that
        wallet. This periodic REST comparison against the previous snapshot
        is the only remaining signal for "target closed a position with no
        fill we saw" in that case, restoring the safety net's documented
        purpose independent of WS delivery.
        """
        import random
        _FEED_STALE_SECS = 300  # 5 minutes with a connection that claims open but sent nothing
        _stale_alerted = False
        await asyncio.sleep(random.uniform(0, 30))  # initial stagger
        while True:
            await asyncio.sleep(50 + random.uniform(0, 20))  # 50–70s jitter
            try:
                # Feed-health: distinguishes "target is just idle" from "the
                # feed died" (a connection can report is_running=True while
                # silently receiving nothing, e.g. a half-open TCP connection).
                # last_ws_event_ts == 0 means no event has arrived since this
                # monitor started, which the subscribe-time userFills snapshot
                # (see _handle_user_fills_event) makes normal at startup — only
                # alert once an established feed has actually gone quiet, and
                # only once per stale episode (a live event resets the flag).
                if self.last_ws_event_ts > 0:
                    age = time.time() - self.last_ws_event_ts
                    if age > _FEED_STALE_SECS and self.ws and self.ws.is_running:
                        if not _stale_alerted:
                            _stale_alerted = True
                            logger.warning(
                                f"Feed stale for {self.target_address[:10]}…: no WS event in "
                                f"{age:.0f}s despite an open connection"
                            )
                            if self.on_alert:
                                self.on_alert(
                                    f"Feed stale: no WS event in {age/60:.1f}min despite an "
                                    f"open connection — the socket may be half-open"
                                )
                    else:
                        _stale_alerted = False

                prev_positions = {p.symbol: p for p in self.last_positions}
                await self.get_current_state()
                logger.debug(f"State refreshed for {self.target_address[:10]}…")
                if self.current_state is not None and self.on_position_close:
                    new_symbols = {p.symbol for p in self.current_state.positions}
                    for sym in prev_positions:
                        if sym in new_symbols:
                            continue
                        pos_data = {"coin": sym}
                        try:
                            if asyncio.iscoroutinefunction(self.on_position_close):
                                await self.on_position_close(pos_data)
                            else:
                                self.on_position_close(pos_data)
                        except Exception as e:
                            logger.error(f"REST-driven position-close callback error: {e}")
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

            # Stamp AFTER the address check, not on every message this shared
            # connection sees — feed_age_secs must reflect events actually
            # about THIS wallet, so a connection shared with a busier wallet
            # doesn't mask this one's feed having gone silent.
            self.last_ws_event_ts = time.time()

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
        self.last_ws_event_ts = time.time()
        try:
            inner = update.data.get("data", {})
            fills = inner.get("fills", [])
            if not fills:
                return
            if inner.get("isSnapshot"):
                # HL resends the target's recent HISTORICAL fills on every
                # (re)subscribe. They must never be copied as live trades: on a
                # fresh DB the tid dedup has never seen them, so hours-old fills
                # would be "copied" at their original prices the instant the
                # wallet connects, minting phantom PnL (violates "copy from
                # now" — startup seeding already mirrors pre-existing positions
                # at current mark). First snapshot after process start
                # (_last_fill_time == 0) is pure history: advance the fill
                # clock past it and drop everything. A reconnect snapshot
                # (_last_fill_time > 0) keeps only fills newer than the clock,
                # preserving its role as a missed-fill backstop for disconnect
                # gaps (tid dedup downstream absorbs overlap with the REST
                # replay).
                if self._last_fill_time == 0:
                    newest = max((f.get("time", 0) for f in fills), default=0)
                    if newest > self._last_fill_time:
                        self._last_fill_time = newest
                    logger.info(
                        f"userFills snapshot: skipped {len(fills)} historical "
                        f"fill(s) for {self.target_address[:10]}… (copy-from-now)"
                    )
                    return
                fills = [f for f in fills if f.get("time", 0) > self._last_fill_time]
                if not fills:
                    return
            await self._handle_fills(fills)
        except Exception as e:
            logger.error(f"Error handling userFills WS event: {e}")

    async def _handle_fills(self, fills: List[dict]):
        from config.settings import settings
        _clock_before = self._last_fill_time
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
            # Enqueue in arrival order — the single consumer task (started in
            # start_monitoring) processes fills for this wallet strictly FIFO.
            # put_nowait never blocks/drops: the queue is unbounded, so a fast
            # burst (HFT target, or a post-reconnect replay of up to 2000
            # fills) still enqueues every fill instantly.
            self._fill_queue.put_nowait(fill)

        if self._last_fill_time > _clock_before:
            self._maybe_persist_fill_clock()

    def _maybe_persist_fill_clock(self):
        """Throttled (≤1/5s) persist of the fill-clock watermark to the DB so
        a restart can restore it — see the note on __init__'s
        _last_fill_time. Fire-and-forget: a missed persist just means a
        slightly larger (still correct, still bounded by the replay cap)
        replay window after the next restart, never data loss."""
        now = time.monotonic()
        if now - self._last_fill_persist_ts < 5.0:
            return
        self._last_fill_persist_ts = now
        ts = self._last_fill_time
        asyncio.create_task(self._persist_fill_clock(ts))

    async def _persist_fill_clock(self, ts_ms: int):
        try:
            from web.db import db_update_last_fill_time
            await asyncio.to_thread(db_update_last_fill_time, self.target_address.lower(), ts_ms)
        except Exception as e:
            logger.debug(f"Fill clock persist failed (non-fatal): {e}")

    async def _consume_fills(self):
        """The single consumer of self._fill_queue for this wallet — awaits
        on_order_fill sequentially so fills apply in exact arrival order
        (and therefore per-symbol order) within this wallet's stream, while
        other wallets' consumers run fully in parallel. One bad fill must
        never kill the stream, so each iteration is wrapped individually."""
        while True:
            fill = await self._fill_queue.get()
            try:
                if self.on_order_fill:
                    if asyncio.iscoroutinefunction(self.on_order_fill):
                        await self.on_order_fill(fill)
                    else:
                        self.on_order_fill(fill)
            except Exception as e:
                logger.error(f"Fill callback error: {e}")
            finally:
                self._fill_queue.task_done()

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
                # 2000 = Hyperliquid's userFills REST endpoint's own max return size,
                # so this cap only ever bites when the gap genuinely exceeds what a
                # single non-time-bounded request can return at all. Algorithmic
                # targets can blow through the old 500-fill cap in minutes.
                if len(all_new) > _REPLAY_FILL_CAP:
                    dropped = len(all_new) - _REPLAY_FILL_CAP
                    logger.warning(
                        f"Replay capped at {_REPLAY_FILL_CAP} fills (had {len(all_new)}) — likely HFT target"
                    )
                    if self.on_alert:
                        self.on_alert(
                            f"Replay truncated for {self.target_address[:10]}…: "
                            f"{dropped} older missed fill(s) were dropped, not copied"
                        )
                    all_new = sorted(all_new, key=lambda f: f.get("time", 0))[-_REPLAY_FILL_CAP:]
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
