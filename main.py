import logging
import asyncio
from datetime import datetime as dt
from zoneinfo import ZoneInfo
from collections import deque
from typing import Optional, List, Tuple
from grpc import ssl_channel_credentials, StatusCode
from grpc.aio import secure_channel, AioRpcError
from clickhouse_connect import get_async_client

from config import settings

from finam_grpc.tradeapi.v1.auth.auth_service_pb2 import SubscribeJwtRenewalRequest
from finam_grpc.tradeapi.v1.auth.auth_service_pb2_grpc import AuthServiceStub
from finam_grpc.tradeapi.v1.marketdata.marketdata_service_pb2 import SubscribeLatestTradesRequest
from finam_grpc.tradeapi.v1.marketdata.marketdata_service_pb2_grpc import MarketDataServiceStub

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

logger = logging.getLogger("quotes_collector")


class DataStore:
    """Центральное хранилище данных с управлением подключениями"""
    
    def __init__(self) -> None:
        self.channel = None
        self.metadata: Optional[List[Tuple[str, str]]] = None
        self.jwt_ready_event = asyncio.Event()
        self._update_token: Optional[asyncio.Task] = None
        self._is_running = False
        self._ch_client = None
        self._ch_client_lock = asyncio.Lock()

    async def _create_ch_client(self):
        """Создаёт клиент ClickHouse (вызывается один раз)"""
        return await get_async_client(
            host=settings.DB_HOST,
            port=8123,
            username="default",
            password=settings.DB_PASSWORD,
            database="default",
            connect_timeout=5,
            settings={
                'async_insert': 1,
                'wait_for_async_insert': 0,
                'max_execution_time': 10,
                'max_block_size': 2000,
                'prefer_localhost_replica': 1,
                'use_uncompressed_cache': 1,
                'load_balancing': 'random'
            }
        )

    async def get_client(self):
        """
        Возвращает переиспользуемый клиент ClickHouse.
        Создаёт его лениво при первом вызове; при обрыве соединения
        пересоздаёт автоматически.
        """
        async with self._ch_client_lock:
            if self._ch_client is None:
                self._ch_client = await self._create_ch_client()
                logger.info("ClickHouse client created")
        return self._ch_client

    async def reset_ch_client(self):
        """Принудительно пересоздать клиент (например, после ошибки соединения)"""
        async with self._ch_client_lock:
            if self._ch_client is not None:
                try:
                    await self._ch_client.close()
                except Exception as e:
                    logger.warning(f"Error closing stale ClickHouse client: {e}")
            self._ch_client = await self._create_ch_client()
            logger.info("ClickHouse client reset")

    async def close_ch_client(self):
        async with self._ch_client_lock:
            if self._ch_client is not None:
                try:
                    await self._ch_client.close()
                    logger.info("ClickHouse client closed")
                except Exception as e:
                    logger.warning(f"Error closing ClickHouse client: {e}")
                finally:
                    self._ch_client = None

    async def _update_token_loop(self):
        """Фоновое обновление JWT токена"""
        stub = AuthServiceStub(self.channel)
        request = SubscribeJwtRenewalRequest(secret=settings.FINAM_TOKEN)
        
        while self._is_running:
            try:
                stream =  stub.SubscribeJwtRenewal(request)
                logger.info("JWT token renewal stream started")
                async for msg in stream:
                    self.metadata = [('authorization', f'Bearer {msg.token}')]
                    self.jwt_ready_event.set()
                    logger.debug("JWT token updated successfully")
            except AioRpcError as e:
                if e.code() == StatusCode.RESOURCE_EXHAUSTED:
                    logger.warning("JWT stream resource exhausted, waiting 60s...")
                    await asyncio.sleep(60)
                else:
                    logger.error(f"JWT stream gRPC error: {e}")
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.info("JWT token update task cancelled")
                break
            except Exception as e:
                logger.error(f"JWT stream unexpected error: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def start_data_updates(self):
        """Запуск всех фоновых задач"""
        if self._is_running:
            logger.warning("Data updates already running")
            return
            
        self._is_running = True
        
        if self.channel is None:
            self.channel = secure_channel(
                settings.FINAM_HOST, 
                ssl_channel_credentials(),
                options=[
                    ('grpc.max_send_message_length', 1024 * 1024),
                    ('grpc.max_receive_message_length', 1024 * 1024),
                    ('grpc.keepalive_time_ms', 10000),
                    ('grpc.keepalive_timeout_ms', 5000),
                    ('grpc.keepalive_permit_without_calls', True),
                    ('grpc.http2.max_pings_without_data', 0),
                    ('grpc.http2.min_time_between_pings_ms', 10000),
                    ('grpc.http2.max_ping_strikes', 2),
                    ('grpc.max_concurrent_streams', 100),
                    ('grpc.use_local_subchannel_pool', 1)
                ]
            )
            logger.info("gRPC channel created")

        if self._update_token is None or self._update_token.done():
            self._update_token = asyncio.create_task(self._update_token_loop())
            logger.info("JWT token update task started")

    async def stop_data_updates(self):
        """Остановка всех фоновых задач"""
        self._is_running = False

        if self._update_token and not self._update_token.done():
            self._update_token.cancel()
            try:
                await self._update_token
            except asyncio.CancelledError:
                pass
            logger.info("JWT token update task stopped")

        if self.channel:
            try:
                await self.channel.close()
                logger.info("gRPC channel closed")
            except Exception as e:
                logger.error(f"Error closing gRPC channel: {e}")

        await self.close_ch_client()


class TradesCollector:
    """Коллектор данных о сделках"""
    
    def __init__(self, asset: str, data_store: DataStore) -> None:
        self.asset = asset
        self.trades_count = 0
        self.batch_size = 1000
        self.buffer = deque(maxlen=1000)
        self.data_store = data_store
        self._update_trades: Optional[asyncio.Task] = None
        self._is_running = False

    async def create_db(self):
        """Создание таблицы в ClickHouse"""
        try:
            client = await self.data_store.get_client()
            await client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.asset[:-5]} (
                price Float64 CODEC(Gorilla, LZ4),
                timestamp Float64 CODEC(Gorilla, LZ4),
                size_value Int64 CODEC(Delta, LZ4),
                open_interest Float64 CODEC(Delta, LZ4),
                trade_id String CODEC(LZ4)
            )
            ENGINE = ReplacingMergeTree()
            PARTITION BY toYYYYMM(toDateTime(timestamp))
            ORDER BY (trade_id, timestamp)
            """)
            logger.info(f"Table for {self.asset} created/verified successfully")
        except Exception as e:
            logger.error(f"Error creating table for {self.asset}: {e}", exc_info=True)
            raise

    async def save_to_db(self) -> bool:
        """Сохранение буфера в базу данных"""
        if not self.buffer:
            return True

        try:
            client = await self.data_store.get_client()
            await client.insert(
                self.asset[:-5],
                list(self.buffer),
                column_names=["price", "timestamp", "size_value", "open_interest", "trade_id"],
            )
            logger.info(f"Saved {len(self.buffer)} trades for {self.asset}")
            return True
        except Exception as e:
            logger.error(f"Error saving to DB for {self.asset}: {e}", exc_info=True)

            try:
                await self.data_store.reset_ch_client()
            except Exception as reset_err:
                logger.error(f"Error resetting ClickHouse client: {reset_err}")
            return False

    async def latest_trades(self):
        """Подписка на поток последних сделок"""
        stub = MarketDataServiceStub(self.data_store.channel)
        request = SubscribeLatestTradesRequest(symbol=self.asset)
        
        while self._is_running:
            try:
                stream = stub.SubscribeLatestTrades(request, metadata=self.data_store.metadata)
                logger.info(f"Trade stream started for {self.asset}")
                async for msg in stream:
                    for trade in msg.trades:
                    
                        row = (
                            float(trade.price.value),
                            trade.timestamp.seconds + trade.timestamp.nanos / 1e9,
                            int(float(trade.size.value)) if trade.side == 1 else -int(float(trade.size.value)),
                            int(float(trade.open_interest.value)),
                            trade.trade_id
                        )
                        
                        self.buffer.append(row)
                        self.trades_count += 1
                        
                        if self.trades_count >= self.batch_size:
                            if await self.save_to_db():
                                          self.trades_count = 0

            except AioRpcError as e:
                if e.code() == StatusCode.RESOURCE_EXHAUSTED:
                    logger.warning(f"Trade stream resource exhausted for {self.asset}, waiting 60s...")
                    await asyncio.sleep(60)
                else:
                    logger.error(f"Trade stream gRPC error for {self.asset}: {e}")
                    await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Trade stream unexpected error for {self.asset}: {e}", exc_info=True)
                # Сохраняем накопленные данные при ошибке
                if self.buffer:
                    await self.save_to_db()
                await asyncio.sleep(5)

    async def start_updates(self):
        """Запуск сбора данных"""
        if self._is_running:
            logger.warning(f"Updates already running for {self.asset}")
            return
            
        self._is_running = True
        await self.create_db()
        
        # Ждем получения JWT токена
        try:
            await asyncio.wait_for(self.data_store.jwt_ready_event.wait(), timeout=60)
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for JWT token for {self.asset}")
            self._is_running = False
            raise
        
        if self._update_trades is None or self._update_trades.done():
            self._update_trades = asyncio.create_task(self.latest_trades())
            logger.info(f"Trade collection started for {self.asset}")

    async def stop_updates(self):
        """Остановка сбора данных"""
        self._is_running = False
        
        if self._update_trades and not self._update_trades.done():
            self._update_trades.cancel()
            try:
                await self._update_trades
            except asyncio.CancelledError:
                pass
            logger.info(f"Trade collection stopped for {self.asset}")

        # Сохраняем остатки буфера
        if self.buffer:
            await self.save_to_db()
            logger.info(f"Final buffer saved for {self.asset}")


class TradingSessionManager:
    """Менеджер торговых сессий"""
    
    def __init__(self):
        self.data_store: Optional[DataStore] = None
        self.collectors: List[TradesCollector] = []
        self._is_running = False
        
    async def start_session(self):
        """Запуск торговой сессии"""
        if self._is_running:
            logger.warning("Trading session already running")
            return
        
        try:    
            self._is_running = True
            self.data_store = DataStore()
            await self.data_store.start_data_updates()
            
            # Список активов для сбора
            assets = ["SiU6@RTSX", "CRU6@RTSX", "MXU6@RTSX", "GDU6@RTSX", "BRV6@RTSX", "NGU6@RTSX", "RIU6@RTSX", "CCX6@RTSX", "PDU6@RTSX", "BTU6@RTSX"]
            
            for asset in assets:
                collector = TradesCollector(asset, self.data_store)
                await collector.start_updates()
                self.collectors.append(collector)
                
            logger.info(f"Trading session started with {len(assets)} assets")
        except Exception:
            logger.error("Failed to start trading session, rolling back", exc_info=True)
            await self.stop_session()
            raise
        
    async def stop_session(self):
        """Остановка торговой сессии"""
        if not self._is_running:
            return
            
        self._is_running = False
        
        # Останавливаем всех коллекторов
        for collector in self.collectors:
            await collector.stop_updates()
            
        # Останавливаем DataStore
        if self.data_store:
            await self.data_store.stop_data_updates()
            
        self.collectors.clear()
        logger.info("Trading session stopped")


async def main():
    """Основная функция"""
    session_manager = TradingSessionManager()
    
    while True:
        try:
            current_hour = dt.now(ZoneInfo("Europe/Moscow")).hour
            
            if current_hour > 6:  # Рабочее время
                if not session_manager._is_running:
                    await session_manager.start_session()
                await asyncio.sleep(60)  # Проверка каждую минуту
            else:
                if session_manager._is_running:
                    await session_manager.stop_session()
                logger.info("Outside trading hours, waiting...")
                await asyncio.sleep(300)  # Проверка каждые 5 минут
                
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Application crashed: {e}", exc_info=True)