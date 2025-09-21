import ccxt
import time
import threading
import logging
import os
import itertools
from typing import Dict, List, Optional, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Глобальный кэш для хранения последних данных.
GLOBAL_ARB_DATA: Dict[str, List[Dict]] = {
    'latest_spreads': []
}

# Список бирж, которые мы поддерживаем
SUPPORTED_EXCHANGES = [
    'mexc', 'lbank', 'binance', 'bybit', 'kucoin', 'gateio', 'bingx'
]
DEFAULT_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'MEME/USDT', 'RWA/USDT']


class ArbAnalyzer(threading.Thread):
    SUPPORTED_EXCHANGES = [ex.upper() for ex in SUPPORTED_EXCHANGES]  # Для API

    def __init__(self, update_interval: int = 3):
        super().__init__()
        self.daemon = True
        self.update_interval = update_interval
        self._stop_event = threading.Event()
        self.exchanges = self._initialize_exchanges()
        logging.info(f"Инициализированы биржи: {list(self.exchanges.keys())}")

    def _initialize_exchanges(self) -> Dict[str, ccxt.Exchange]:
        """Инициализирует объекты CCXT."""
        instances = {}
        for ex_id in SUPPORTED_EXCHANGES:
            try:
                # В реальном приложении API-ключи должны загружаться из безопасного хранилища
                # Для примера CCXT может работать в режиме публичного доступа
                exchange = getattr(ccxt, ex_id)({})
                if exchange.has['fetchFundingRate']:
                    instances[ex_id] = exchange
            except Exception as e:
                logging.warning(f"Не удалось инициализировать {ex_id}: {e}")
        return instances

    def _calculate_spread(self,
                          symbol: str,
                          ex_long_id: str,
                          ex_short_id: str) -> Optional[Dict[str, Any]]:
        """
        Производит расчет чистого спреда.
        """
        ex_long = self.exchanges.get(ex_long_id)
        ex_short = self.exchanges.get(ex_short_id)
        if not ex_long or not ex_short:
            return None

        try:
            # Получаем книгу ордеров (Bid/Ask)
            orderbook_long = ex_long.fetch_order_book(symbol)
            orderbook_short = ex_short.fetch_order_book(symbol)

            # Получаем ставку фандинга
            funding_long = ex_long.fetch_funding_rate(symbol)['fundingRate']
            funding_short = ex_short.fetch_funding_rate(symbol)['fundingRate']

            # Определение Long/Short: Long - там, где Bid ниже; Short - там, где Ask выше.
            bid_long = orderbook_long['bids'][0][0]
            ask_short = orderbook_short['asks'][0][0]

            # Расчет грубого спреда (%)
            if bid_long <= 0 or ask_short <= 0: return None

            gross_spread_abs = ask_short - bid_long
            gross_spread_pct = (gross_spread_abs / bid_long) * 100

            # Вычет комиссий (0.04% Taker для примера) и учет фандинга
            commission_rate = 0.0004
            total_commission_pct = commission_rate * 2 * 100
            funding_difference = funding_short - funding_long

            # Формула чистого спреда
            net_spread_pct = gross_spread_pct - total_commission_pct + (funding_difference * 100)

            # Формирование результата для UI
            return {
                'coin': symbol.split('/')[0],
                'long_exchange': ex_long_id.upper(),
                'short_exchange': ex_short_id.upper(),
                'bid_long': bid_long,
                'ask_short': ask_short,
                'gross_spread': round(gross_spread_pct, 4),
                'net_spread': round(net_spread_pct, 4),
                'funding_long': round(funding_long * 100, 4),
                'funding_short': round(funding_short * 100, 4),
                'status': '🟢 Актуально' if net_spread_pct >= 0.01 else '🟡 Неактуально',
            }

        except Exception as e:
            # logging.error(f"Ошибка расчета связки {symbol} ({ex_long_id}/{ex_short_id}): {e}")
            return None

    def stop(self):
        """Останавливает поток."""
        self._stop_event.set()

    def run(self):
        """Основной цикл сбора и анализа данных."""
        while not self._stop_event.is_set():
            start_time = time.time()
            all_actual_spreads = []

            for symbol in DEFAULT_SYMBOLS:
                # Итерация по всем возможным парам бирж в обе стороны
                exchange_ids = list(self.exchanges.keys())
                for ex_long_id, ex_short_id in itertools.permutations(exchange_ids, 2):

                    result = self._calculate_spread(symbol, ex_long_id, ex_short_id)

                    if result and result['net_spread'] > 0.01:
                        all_actual_spreads.append(result)

            GLOBAL_ARB_DATA['latest_spreads'] = all_actual_spreads
            logging.info(f"Обновлено {len(all_actual_spreads)} связок. Время: {time.time() - start_time:.2f}s")

            # Ждем до следующего обновления
            time_to_wait = self.update_interval - (time.time() - start_time)
            if time_to_wait > 0:
                time.sleep(time_to_wait)