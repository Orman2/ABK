import ccxt
import time
import threading
import logging
from typing import Dict, List, Optional
import os
import itertools  # Для генерации пар бирж

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Глобальный кэш для хранения последних данных.
# К нему будет обращаться Flask для формирования UI.
GLOBAL_ARB_DATA: Dict[str, List[Dict]] = {
    'latest_spreads': []  # Список всех актуальных связок
}

# Список бирж, которые мы поддерживаем (должен совпадать с вашими API)
SUPPORTED_EXCHANGES = [
    'mexc', 'lbank', 'binance', 'bybit', 'kucoin', 'gateio', 'bingx'
]
# Список монет, которые мы отслеживаем по умолчанию
DEFAULT_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'MEME/USDT', 'RWA/USDT']


# Ваша логика должна определять, какие пары являются фьючерсами ('ETH/USDT:USDT')

class ArbAnalyzer(threading.Thread):
    def __init__(self, update_interval: int = 3):
        super().__init__()
        self.daemon = True
        self.update_interval = update_interval
        self.exchanges = self._initialize_exchanges()
        logging.info(f"Инициализированы биржи: {list(self.exchanges.keys())}")

    def _initialize_exchanges(self) -> Dict[str, ccxt.Exchange]:
        """Инициализирует объекты CCXT для бирж, где доступны API ключи."""
        instances = {}
        for ex_id in SUPPORTED_EXCHANGES:
            # В реальном приложении API-ключи должны загружаться из безопасного хранилища
            # Для простоты, мы проверяем только наличие токена (если он нужен для публичного доступа)
            try:
                # Настройки API-ключей (заполните, если нужно, иначе будет только публичный доступ)
                api_key = os.getenv(f"{ex_id.upper()}_API_KEY")
                secret = os.getenv(f"{ex_id.upper()}_SECRET")

                exchange = getattr(ccxt, ex_id)({
                    'apiKey': api_key,
                    'secret': secret,
                    # Можно добавить прокси или другие настройки
                })
                # Проверка, что фьючерсы доступны
                if exchange.has['fetchFundingRate']:
                    instances[ex_id] = exchange
            except Exception as e:
                logging.warning(f"Не удалось инициализировать {ex_id}: {e}")
        return instances

    def _calculate_spread(self,
                          symbol: str,
                          ex_long_id: str,
                          ex_short_id: str) -> Optional[Dict]:
        """
        Производит расчет чистого спреда между двумя биржами.
        (Здесь будет сложная логика из вашего ТЗ)
        """

        # 1. Получение данных (Bid/Ask) из кэша.
        # *ВНИМАНИЕ: Для этого примера мы пока пропустим фазу кэша и сделаем запрос напрямую.*

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

            # 2. Определяем Long/Short (где Bid ниже - там Long, где Ask выше - там Short)
            # Мы упростим и скажем, что Long - это ex_long, Short - это ex_short
            bid_long = orderbook_long['bids'][0][0]  # Лучшая цена Bid на Long-бирже
            ask_short = orderbook_short['asks'][0][0]  # Лучшая цена Ask на Short-бирже

            # 3. Расчет грубого спреда (%)
            if bid_long <= 0 or ask_short <= 0: return None

            gross_spread_abs = ask_short - bid_long
            gross_spread_pct = (gross_spread_abs / bid_long) * 100

            # 4. ВЫЧЕТ КОМИССИЙ И ФАНДИНГА (Место для вашей сложной формулы!)
            # Здесь должны быть ваши комиссии. Возьмем примерные 0.04% на сделку (Taker)
            commission_rate = 0.0004
            total_commission = commission_rate * 2 * 100  # за обе сделки в %

            funding_difference = funding_short - funding_long  # Разница фандинга

            # Пример чистого спреда:
            net_spread_pct = gross_spread_pct - total_commission + (funding_difference * 100)

            # 5. Формирование результата для UI
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
                'status': '🟢 Актуально' if net_spread_pct >= 0.5 else '🟡 Неактуально',
                # Условно, если > 0.5% чистой прибыли
                # ... добавить время фандинга, объем и т.д.
            }

        except Exception as e:
            # В случае ошибки API или отсутствия пары
            logging.error(f"Ошибка расчета связки {symbol} ({ex_long_id}/{ex_short_id}): {e}")
            return None

    def run(self):
        """Основной цикл сбора и анализа данных."""
        while True:
            start_time = time.time()
            all_actual_spreads = []

            # Итерация по всем монетам и всем возможным парам бирж
            for symbol in DEFAULT_SYMBOLS:
                # itertools.combinations('mexc', 'lbank') -> [('mexc', 'lbank')]
                for ex_long_id, ex_short_id in itertools.combinations(self.exchanges.keys(), 2):

                    # Расчет в обе стороны (Long/Short)
                    for long_id, short_id in [(ex_long_id, ex_short_id), (ex_short_id, ex_long_id)]:
                        result = self._calculate_spread(symbol, long_id, short_id)

                        if result and result['net_spread'] > 0.01:  # Фильтруем все, что не приносит прибыль
                            all_actual_spreads.append(result)

            # Обновляем глобальный кэш, к которому обратится Flask
            GLOBAL_ARB_DATA['latest_spreads'] = all_actual_spreads
            logging.info(f"Обновлено {len(all_actual_spreads)} связок. Время: {time.time() - start_time:.2f}s")

            # Ждем до следующего обновления
            time_to_wait = self.update_interval - (time.time() - start_time)
            if time_to_wait > 0:
                time.sleep(time_to_wait)