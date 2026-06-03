"""Trade execution engine for Hyperliquid"""
import time
from typing import Optional, Dict, Any
from decimal import Decimal
from eth_account import Account
import aiohttp

from utils.logger import logger
from hyperliquid.models import OrderType, OrderSide


class TradeExecutor:
    """Executes trades on Hyperliquid exchange"""

    def __init__(
        self,
        wallet_address: str,
        private_key: str,
        info_url: str = "https://api.hyperliquid.xyz/info",
        exchange_url: str = "https://api.hyperliquid.xyz/exchange",
        dry_run: bool = True
    ):
        self.wallet_address = wallet_address.lower() if wallet_address else None
        self.private_key = private_key
        self.info_url = info_url
        self.exchange_url = exchange_url
        self.dry_run = dry_run
        self._coin_index_cache: Dict[str, int] = {}

        # Initialize signing account if we have credentials
        self.account = None
        if self.private_key and not self.dry_run:
            try:
                self.account = Account.from_key(self.private_key)
                # Validate address matches
                if self.account.address.lower() != self.wallet_address:
                    raise ValueError(
                        f"Private key address {self.account.address} doesn't match "
                        f"configured address {self.wallet_address}"
                    )
                logger.info(f"✅ Executor initialized for wallet {self.wallet_address}")
            except Exception as e:
                logger.error(f"Failed to initialize signing account: {e}")
                raise
        elif not self.dry_run:
            raise ValueError("Cannot run in live mode without private key")
        else:
            logger.warning("⚠️ Running in DRY RUN mode - no real trades will be executed")

    async def _get_asset_index(self, symbol: str) -> int:
        """Resolve a coin symbol to its integer asset index.

        Hyperliquid's exchange endpoint requires an integer for the 'asset' /
        'a' fields, not a coin name string. The index is the coin's position in
        the universe array returned by the meta endpoint.
        """
        if not self._coin_index_cache:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.info_url,
                    json={"type": "meta"},
                    headers={"Content-Type": "application/json"}
                ) as response:
                    data = await response.json()
            universe = data.get("universe", [])
            self._coin_index_cache = {coin["name"]: i for i, coin in enumerate(universe)}

        if symbol not in self._coin_index_cache:
            raise ValueError(f"Unknown asset symbol: {symbol}")
        return self._coin_index_cache[symbol]

    def _sign_action(self, action: Dict[str, Any]) -> Dict[str, Any]:
        """Sign an action using EIP-712 structured data signing"""
        if not self.account:
            raise ValueError("Cannot sign actions without account")

        timestamp = int(time.time() * 1000)

        structured_data = {
            "domain": {
                "name": "Exchange",
                "version": "1",
                "chainId": 1337,
                "verifyingContract": "0x0000000000000000000000000000000000000000"
            },
            "primaryType": "Agent",
            "types": {
                "Agent": [
                    {"name": "source", "type": "string"},
                    {"name": "connectionId", "type": "bytes32"}
                ],
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"}
                ]
            },
            "message": {
                "source": "a",
                "connectionId": "0x" + "0" * 64
            }
        }

        signed_message = self.account.sign_typed_data(
            structured_data["domain"],
            {"Agent": structured_data["types"]["Agent"]},
            structured_data["message"]
        )

        signature = {
            "r": "0x" + signed_message.r.to_bytes(32, "big").hex(),
            "s": "0x" + signed_message.s.to_bytes(32, "big").hex(),
            "v": signed_message.v
        }

        return {
            "action": action,
            "nonce": timestamp,
            "signature": signature,
            "vaultAddress": None
        }

    async def _update_leverage(
        self,
        symbol: str,
        leverage: int,
        is_cross: bool = True
    ) -> bool:
        try:
            asset_index = await self._get_asset_index(symbol)
            action = {
                "type": "updateLeverage",
                "asset": asset_index,
                "isCross": is_cross,
                "leverage": leverage
            }

            signed_action = self._sign_action(action)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.exchange_url,
                    json=signed_action,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        await response.json()
                        logger.success(f"✅ Updated leverage for {symbol} to {leverage}x")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to update leverage: {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Error updating leverage: {e}")
            return False

    async def execute_market_order(
        self,
        symbol: str,
        side: OrderSide,
        size: Decimal,
        leverage: int = 1,
        reduce_only: bool = False
    ) -> Optional[str]:
        if self.dry_run:
            return await self._simulate_order(
                symbol=symbol,
                side=side,
                size=size,
                order_type=OrderType.MARKET,
                leverage=leverage
            )

        try:
            if leverage > 1:
                await self._update_leverage(symbol, leverage)

            asset_index = await self._get_asset_index(symbol)
            action = {
                "type": "order",
                "orders": [{
                    "a": asset_index,
                    "b": side == OrderSide.BUY,
                    "p": "0",
                    "s": str(float(size)),
                    "r": reduce_only,
                    "t": {"limit": {"tif": "Ioc"}},
                    "c": None
                }],
                "grouping": "na"
            }

            signed_action = self._sign_action(action)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.exchange_url,
                    json=signed_action,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.success(
                            f"✅ Market {side.value} order executed: {symbol} "
                            f"size={size} leverage={leverage}x"
                        )
                        if result.get("status") == "ok" and result.get("response", {}).get("data"):
                            order_id = result["response"]["data"].get("statuses", [{}])[0].get("resting", {}).get("oid")
                            return order_id
                        return "executed"
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to execute market order: {error_text}")
                        return None

        except Exception as e:
            logger.error(f"Error executing market order: {e}")
            return None

    async def execute_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        size: Decimal,
        price: Decimal,
        leverage: int = 1,
        reduce_only: bool = False,
        post_only: bool = False
    ) -> Optional[str]:
        if self.dry_run:
            return await self._simulate_order(
                symbol=symbol,
                side=side,
                size=size,
                order_type=OrderType.LIMIT,
                price=price,
                leverage=leverage
            )

        try:
            if leverage > 1:
                await self._update_leverage(symbol, leverage)

            asset_index = await self._get_asset_index(symbol)
            tif = "Alo" if post_only else "Gtc"

            action = {
                "type": "order",
                "orders": [{
                    "a": asset_index,
                    "b": side == OrderSide.BUY,
                    "p": str(float(price)),
                    "s": str(float(size)),
                    "r": reduce_only,
                    "t": {"limit": {"tif": tif}},
                    "c": None
                }],
                "grouping": "na"
            }

            signed_action = self._sign_action(action)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.exchange_url,
                    json=signed_action,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.success(
                            f"✅ Limit {side.value} order placed: {symbol} "
                            f"size={size} price={price} leverage={leverage}x"
                        )
                        if result.get("status") == "ok" and result.get("response", {}).get("data"):
                            order_id = result["response"]["data"].get("statuses", [{}])[0].get("resting", {}).get("oid")
                            return order_id
                        return None
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to place limit order: {error_text}")
                        return None

        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
            return None

    async def close_position(
        self,
        symbol: str,
        size: Optional[Decimal] = None,
        side: Optional[OrderSide] = None
    ) -> Optional[str]:
        if self.dry_run:
            if size and side:
                logger.info(f"🔵 DRY RUN: Would close {side.value} {size} {symbol}")
            else:
                logger.info(f"🔵 DRY RUN: Would close position {symbol}")
            return f"dry_run_close_{symbol}_{int(time.time())}"

        if size is None or side is None:
            logger.warning(f"⚠️ Size and/or side not provided for {symbol}, using reduce_only market order")
            return await self.execute_market_order(
                symbol=symbol,
                side=OrderSide.SELL,
                size=Decimal("0.001"),
                reduce_only=True
            )

        return await self.execute_market_order(
            symbol=symbol,
            side=side,
            size=size,
            reduce_only=True
        )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        if self.dry_run:
            logger.info(f"🔵 DRY RUN: Would cancel order {order_id} for {symbol}")
            return True

        try:
            asset_index = await self._get_asset_index(symbol)
            action = {
                "type": "cancel",
                "cancels": [{
                    "a": asset_index,
                    "o": order_id
                }]
            }

            signed_action = self._sign_action(action)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.exchange_url,
                    json=signed_action,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        logger.success(f"✅ Cancelled order {order_id} for {symbol}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to cancel order: {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return False

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        if self.dry_run:
            logger.info(f"🔵 DRY RUN: Would cancel all orders{f' for {symbol}' if symbol else ''}")
            return 0

        try:
            asset_index = await self._get_asset_index(symbol) if symbol else None
            action = {
                "type": "cancelByCloid",
                "cancels": [{
                    "asset": asset_index,
                    "cloid": None
                }]
            }

            signed_action = self._sign_action(action)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.exchange_url,
                    json=signed_action,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        count = len(result.get("response", {}).get("data", {}).get("statuses", []))
                        logger.success(f"✅ Cancelled {count} orders{f' for {symbol}' if symbol else ''}")
                        return count
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to cancel all orders: {error_text}")
                        return 0

        except Exception as e:
            logger.error(f"Error cancelling all orders: {e}")
            return 0

    async def _simulate_order(
        self,
        symbol: str,
        side: OrderSide,
        size: Decimal,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        leverage: int = 1
    ) -> str:
        order_id = f"sim_{symbol}_{int(time.time())}"

        if order_type == OrderType.MARKET:
            logger.info(
                f"🔵 DRY RUN: Would execute MARKET {side.value} {symbol} "
                f"size={size} leverage={leverage}x → Order ID: {order_id}"
            )
        else:
            logger.info(
                f"🔵 DRY RUN: Would place LIMIT {side.value} {symbol} "
                f"size={size} price={price} leverage={leverage}x → Order ID: {order_id}"
            )

        return order_id
