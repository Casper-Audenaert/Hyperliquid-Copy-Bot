import asyncio
import json
import websockets
from typing import Optional, Callable, Dict, Any, List
from datetime import datetime
from loguru import logger
from .models import WebSocketUpdate


class HyperliquidWebSocket:
    """
    WebSocket client for real-time Hyperliquid data.

    Designed for shared use: multiple WalletMonitors can subscribe their
    userEvents callbacks on a single connection, staying within HL's
    15-connections-per-IP limit. Dispatch routes "user" channel messages
    to the right callback by matching the `user` field in the message data.
    """

    def __init__(self, ws_url: str = "wss://api.hyperliquid.xyz/ws"):
        self.ws_url = ws_url
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.is_running = False
        self.reconnect_delay = 5
        self.subscriptions: Dict[str, Any] = {}
        self.callbacks: Dict[str, Callable] = {}
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.heartbeat_interval = 55  # Send ping every 55s (server closes at 60s)
        # List of handlers — all are called on reconnect so every monitor can
        # replay missed fills.  Replaces the old single on_reconnect Optional.
        self.on_reconnect_handlers: List[Callable] = []

    async def connect(self):
        """Establish WebSocket connection (idempotent — no-op if already open)."""
        if self.ws and not self.ws.closed:
            return
        try:
            logger.info(f"Connecting to Hyperliquid WebSocket: {self.ws_url}")
            self.ws = await asyncio.wait_for(websockets.connect(self.ws_url), timeout=30)
            self.is_running = True
            logger.info("WebSocket connected successfully")

            if self.heartbeat_task is None or self.heartbeat_task.done():
                self.heartbeat_task = asyncio.create_task(self._heartbeat())

            # Re-subscribe all stored subscriptions after (re)connect
            for sub_data in self.subscriptions.values():
                await self._send_subscription(sub_data)

        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            raise

    async def disconnect(self):
        """Close WebSocket connection."""
        self.is_running = False
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        if self.ws:
            await self.ws.close()
            logger.info("WebSocket disconnected")

    async def _send_subscription(self, data: dict):
        if self.ws:
            try:
                await self.ws.send(json.dumps(data))
                logger.debug(f"Sent subscription: {data}")
            except Exception as e:
                logger.error(f"Failed to send subscription: {e}")

    async def _send_ping(self):
        if self.ws and not self.ws.closed:
            try:
                await self.ws.send(json.dumps({"method": "ping"}))
                logger.debug("❤️ Sent ping")
            except Exception as e:
                logger.error(f"Failed to send ping: {e}")

    async def _heartbeat(self):
        while self.is_running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                if self.is_running and self.ws and not self.ws.closed:
                    await self._send_ping()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def subscribe_user_events(self, address: str, dexs: list,
                                     callback: Optional[Callable] = None):
        """Subscribe to userEvents for a wallet address.

        HL's WS userEvents subscription delivers ALL fills across ALL DEXes in a
        single subscription — no DEX parameter needed or supported.  The DEX loop
        is only required for REST endpoints (fill history replay in monitor.py).
        Sending per-DEX subscription messages causes 'Already subscribed' errors.
        """
        addr_lower = address.lower()
        channel_key = f"user:{addr_lower}"
        if callback:
            self.callbacks[channel_key] = callback

        sub = {
            "method": "subscribe",
            "subscription": {"type": "userEvents", "user": address},
        }
        self.subscriptions[channel_key] = sub
        if self.ws:
            await self._send_subscription(sub)

        logger.info(f"Subscribed userEvents for {address[:10]}…")

    async def unsubscribe_user_events(self, address: str, dexs: list):
        """Remove the subscription and callback for an address."""
        addr_lower = address.lower()
        channel_key = f"user:{addr_lower}"
        self.subscriptions.pop(channel_key, None)
        self.callbacks.pop(channel_key, None)
        if self.ws and not self.ws.closed:
            try:
                await self.ws.send(json.dumps({
                    "method": "unsubscribe",
                    "subscription": {"type": "userEvents", "user": address},
                }))
            except Exception:
                pass
        logger.info(f"Unsubscribed userEvents for {address[:10]}…")

    async def subscribe_user_fills(self, address: str, callback: Optional[Callable] = None):
        """Subscribe to userFills — HL's purpose-built fill feed.  Sends an
        `isSnapshot: true` message with recent fills immediately on (re)subscribe
        (including after a reconnect, via the resubscribe loop in connect()),
        which backstops the REST-based _replay_missed_fills in monitor.py without
        an extra per-DEX REST burst.  Fills are routed through the same
        _handle_fills() path as userEvents, so tid-based dedup already makes
        double-delivery from both feeds safe.
        """
        addr_lower = address.lower()
        channel_key = f"userFills:{addr_lower}"
        if callback:
            self.callbacks[channel_key] = callback

        sub = {
            "method": "subscribe",
            "subscription": {"type": "userFills", "user": address},
        }
        self.subscriptions[channel_key] = sub
        if self.ws:
            await self._send_subscription(sub)

        logger.info(f"Subscribed userFills for {address[:10]}…")

    async def unsubscribe_user_fills(self, address: str):
        addr_lower = address.lower()
        channel_key = f"userFills:{addr_lower}"
        self.subscriptions.pop(channel_key, None)
        self.callbacks.pop(channel_key, None)
        if self.ws and not self.ws.closed:
            try:
                await self.ws.send(json.dumps({
                    "method": "unsubscribe",
                    "subscription": {"type": "userFills", "user": address},
                }))
            except Exception:
                pass
        logger.info(f"Unsubscribed userFills for {address[:10]}…")

    async def _handle_message(self, message: str):
        try:
            data = json.loads(message)
            channel = data.get("channel", "unknown")

            if channel == "pong":
                logger.debug("❤️ Pong received")
                return

            update = WebSocketUpdate(channel=channel, data=data, timestamp=datetime.utcnow())

            if channel == "user":
                # Route "user" channel messages to the correct monitor.
                # HL may include the subscribed user's address in multiple places:
                #   1. inner["user"]  — outer envelope (most reliable)
                #   2. inner["fills"][0]["user"] — each fill carries the user address
                # If neither is present, dispatch to ALL registered user callbacks and
                # let each monitor's _handle_user_event filter by address.
                inner = data.get("data", {})
                user_addr = (inner.get("user") or "").lower()

                if not user_addr:
                    fills = inner.get("fills", [])
                    if fills:
                        user_addr = (fills[0].get("user") or "").lower()

                if user_addr:
                    # Exact-match route to the right monitor
                    callback = self.callbacks.get(f"user:{user_addr}")
                    if callback is not None:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(update)
                            else:
                                callback(update)
                        except Exception as e:
                            logger.error(f"User callback error for {user_addr[:10]}…: {e}")
                    else:
                        logger.debug(f"No callback registered for user={user_addr}")
                else:
                    # No user address found — broadcast to all registered monitors
                    # and let each one filter by its own target address.
                    for key, cb in list(self.callbacks.items()):
                        if not key.startswith("user:"):
                            continue
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(update)
                            else:
                                cb(update)
                        except Exception as e:
                            logger.error(f"User broadcast callback error ({key}): {e}")
                return  # handled above — skip the generic callback path below
            elif channel == "userFills":
                # Same routing pattern as "user": match by embedded user address,
                # falling back to broadcast if HL omits it.
                inner = data.get("data", {})
                user_addr = (inner.get("user") or "").lower()
                if user_addr:
                    callback = self.callbacks.get(f"userFills:{user_addr}")
                    if callback is not None:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(update)
                            else:
                                callback(update)
                        except Exception as e:
                            logger.error(f"userFills callback error for {user_addr[:10]}…: {e}")
                    else:
                        logger.debug(f"No userFills callback registered for user={user_addr}")
                else:
                    for key, cb in list(self.callbacks.items()):
                        if not key.startswith("userFills:"):
                            continue
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(update)
                            else:
                                cb(update)
                        except Exception as e:
                            logger.error(f"userFills broadcast callback error ({key}): {e}")
                return
            else:
                callback = self.callbacks.get(channel)
                if callback is None:
                    for key, cb in self.callbacks.items():
                        if ":" in key and key.split(":")[0] == channel:
                            callback = cb
                            break

            if channel == "subscriptionResponse":
                logger.debug(f"Subscription confirmed: {data.get('data', {})}")
                return

            if channel == "error":
                logger.warning(f"HL WS error: {data.get('data', data)}")
                return

            if callback is not None:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(update)
                    else:
                        callback(update)
                except Exception as e:
                    logger.error(f"Callback error for channel '{channel}': {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            elif channel not in ("subscriptionResponse", "pong"):
                logger.warning(f"No callback for channel: {channel}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WS message: {e}")
        except Exception as e:
            logger.error(f"WS message handling error: {e}")
            import traceback
            logger.error(traceback.format_exc())

    async def listen(self):
        """
        Main listening loop.  Reconnects automatically and fires all registered
        on_reconnect_handlers after every reconnect (for fill-gap replay).
        """
        first_connect = True
        while self.is_running:
            try:
                if not self.ws or self.ws.closed:
                    await self.connect()
                    if not first_connect and self.on_reconnect_handlers:
                        # Stagger handler fan-out: firing all wallets' REST-based fill
                        # replay in the same instant can burst well past HL's rate-limit
                        # budget when many wallets share one reconnecting connection.
                        asyncio.create_task(self._run_reconnect_handlers(list(self.on_reconnect_handlers)))
                    first_connect = False

                async for message in self.ws:
                    await self._handle_message(message)

            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"WS closed, reconnecting in {self.reconnect_delay}s…")
                await asyncio.sleep(self.reconnect_delay)
            except Exception as e:
                logger.error(f"WS listener error: {e}")
                await asyncio.sleep(self.reconnect_delay)

    async def _run_reconnect_handlers(self, handlers: List[Callable]):
        for i, handler in enumerate(handlers):
            if i > 0:
                await asyncio.sleep(0.5)
            asyncio.create_task(handler())

    async def stop(self):
        self.is_running = False
        await self.disconnect()
