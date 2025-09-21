import sqlite3
from typing import List, Dict, Any

DB_NAME = 'arb_settings.db'


def create_db():
    """Создает таблицу настроек пользователя, если она не существует."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            exchanges TEXT,
            blacklist TEXT,
            min_spread REAL,
            min_funding_spread REAL
        )
    ''')
    conn.commit()
    conn.close()


def save_settings(user_id: int, settings: Dict[str, Any]):
    """Сохраняет или обновляет настройки пользователя."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Преобразование списков в строку для хранения (например, 'Binance,Bybit')
    exchanges_str = ",".join(settings.get('exchanges', []))
    blacklist_str = ",".join(settings.get('blacklist', []))

    cursor.execute('''
        INSERT OR REPLACE INTO user_settings 
        (user_id, exchanges, blacklist, min_spread, min_funding_spread) 
        VALUES (?, ?, ?, ?, ?)
    ''', (
        user_id,
        exchanges_str,
        blacklist_str,
        settings.get('min_spread', 3.0),
        settings.get('min_funding_spread', 1.5)
    ))
    conn.commit()
    conn.close()


def get_settings(user_id: int) -> Dict[str, Any]:
    """Загружает настройки пользователя или возвращает дефолтные."""
    create_db()  # Убедимся, что база данных существует
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM user_settings WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    conn.close()

    default_settings = {
        'user_id': user_id,
        'exchanges': [],
        'blacklist': [],
        'min_spread': 3.0,
        'min_funding_spread': 1.5
    }

    if row:
        # Преобразование строк обратно в списки
        return {
            'user_id': row[0],
            'exchanges': row[1].split(',') if row[1] else [],
            'blacklist': row[2].split(',') if row[2] else [],
            'min_spread': row[3],
            'min_funding_spread': row[4]
        }
    return default_settings


if __name__ == '__main__':
    # Пример использования:
    create_db()
    print("База данных готова.")