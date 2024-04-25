import pymysql

from binance.client import Client
from datetime import datetime


def get_market_price(symbol):
    client = Client()
    for asset in client.futures_mark_price():
        if asset['symbol'] == symbol:
            return float(asset['markPrice'])
    return -1


class Trader:
    def __init__(self, trader_id, codename, url):
        self.trader_id = trader_id
        self.codename = codename
        self.url = url


TRADER_IDS = [1, 2, 3, 4, 5, 6, 7, 8]
TRADER_NAMES = ['u5m4d', 'ngekrd', 'jvs8y', 'edcd5', 'wgtx5', '8uhzp', 'zxeuq', 'yf1e6']
TRADER_PROFILE_URLS = [
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=4325641055745EBAFED26DB3ACDC7AF1',
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=B2E4D88D1E5633B2584F87EB5E2A6D6A',
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=F5FB5A0B7B6C7105D86D1D0185D56D21',
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=9C26806355CF586C963188C6E78B2395',
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=06DA95BD25FF79F12661FA72A76D7BCE',
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=84F16B34DDBE53A914F88EFBA67F6E6C',
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=C20E7A8966C0014A4AF5774DD709DC42',
    'https://www.binance.com/en/futures-activity/leaderboard/user/um?encryptedUid=673D76B76B196BE139BCFC6205352078',
]


def get_db_connection():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='complex@123--jj32',
        charset='utf8',
        db='tradewise',
        port=3307,
    )
    return conn


def get_safe_db_connection(connection=None):
    if connection:
        try:
            connection.ping()
            return connection
        except Exception:
            return get_db_connection()
    return get_db_connection()


def save_to_db(data, conn):
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='complex@123--jj32',
        charset='utf8',
        db='tradewise',
        port=3307,
    )
    cur = conn.cursor()

    sql = """
    INSERT INTO binance_positions (trader, symbol, direction, leverage, size, entry_price, action_time, is_open)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.execute(sql, data)
    conn.commit()

    cur.close()


def save_traders_to_db(conn):
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='complex@123--jj32',
        charset='utf8',
        db='tradewise',
        port=3307,
    )
    cur = conn.cursor()

    sql = """
        INSERT INTO traders (trader_id, codename, url)
        VALUES (%s, %s, %s)
        """

    for i in range(len(TRADER_IDS)):
        data = (TRADER_IDS[i], TRADER_NAMES[i], TRADER_PROFILE_URLS[i], 0, 0)
        cur.execute(sql, data)
        conn.commit()

    cur.close()


def save_telegram_signal_to_db(data):
    conn = pymysql.connect(host='localhost', user='root', password='complex@123--jj32', charset='utf8', db='tradewise', port=3307)
    cur = conn.cursor()

    sql = """
        INSERT INTO signal_stats (pair, entry, direction, timestamp)
        VALUES (%s, %s, %s, %s)
        """
    cur.execute(sql, data)
    conn.commit()
    conn.close()


def position_exists(data, conn):
    cur = conn.cursor()

    query = "SELECT * FROM binance_positions WHERE trader = %s AND symbol = %s AND direction = %s;"
    cur.execute(query, data)
    row_exists = cur.fetchall()

    cur.close()
    return row_exists[0][0] if row_exists else -1


def get_position_change(position_id, size, entry_price, conn):
    cur = conn.cursor()

    query = "SELECT * FROM binance_positions WHERE position_id = %s;"
    cur.execute(query, (position_id, ))
    row = cur.fetchone()
    cur.close()

    original_size = row[5]
    original_entry_price = row[6]

    size_change = size / original_size
    entry_price_change = entry_price / original_entry_price

    return {
        'size_change': size_change - 1 if 4 > abs(size_change - 1) >= 0.1 else 0,
        'entry_price_change': entry_price_change - 1 if abs(entry_price_change) >= 0.1 else 0,
    }


def get_position(data, conn):
    cur = conn.cursor()

    query = "SELECT * FROM binance_positions WHERE trader = %s AND symbol = %s AND direction = %s;"
    cur.execute(query, data)
    position = cur.fetchall()
    cur.close()

    return position


def set_row_open(position_id, conn):
    cur = conn.cursor()

    query = "UPDATE binance_positions SET is_open = TRUE WHERE position_id = %s AND is_open = %s;"
    cur.execute(query, (position_id, False))
    conn.commit()

    cur.close()


def set_open_field(conn):
    cur = conn.cursor()
    cur.execute("UPDATE binance_positions SET is_open = FALSE;")
    conn.commit()

    cur.close()


def get_closed_positions(conn):
    cur = conn.cursor()

    query = "SELECT * FROM binance_positions WHERE is_open = FALSE;"
    cur.execute(query)
    positions = cur.fetchall()

    cur.close()
    return positions


def get_all_traders(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM traders_data')

    rows = cursor.fetchall()
    cursor.close()

    traders = []
    for row in rows:
        if row[2]:
            traders.append(
                Trader(
                    trader_id=row[0],
                    codename=row[1],
                    url=row[2],
                ),
            )

    return traders


def create_user(data):
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
            INSERT INTO users_info (user_code, subscription_type, subscription_start, subscription_end, is_enabled_bot, provider, followed_traders, position_percentage, max_positions, max_traders, balance, available_balance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

    cur.execute(query, data)
    conn.commit()
    cur.close()
    conn.close()


def get_users_by_trader(trader_id):
    conn = get_db_connection()
    cur = conn.cursor()

    query = f"SELECT * FROM users_info WHERE followed_traders LIKE '%{trader_id}%';"
    cur.execute(query)
    rows = cur.fetchall()

    cur.close()

    return rows


def delete_trader_positions(trader_id):
    conn = get_db_connection()
    cur = conn.cursor()

    query = "DELETE FROM binance_positions WHERE trader = %s;"
    cur.execute(query, (trader_id,))
    conn.commit()
    cur.close()


def get_trader_positions(trader_id):
    conn = get_db_connection()
    cur = conn.cursor()

    query = "SELECT * FROM binance_positions WHERE trader = %s;"
    cur.execute(query, (trader_id,))
    rows = cur.fetchall()

    cur.close()
    return rows


def create_instruction(data):
    conn = get_db_connection()
    cur = conn.cursor()

    query = """INSERT INTO instructions (trader, symbol, direction, instruction, position_change, size, entry_price, is_done)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

    cur.execute(query, data)
    conn.commit()
    conn.close()
    cur.close()


def update_position_db(position_id, size, entry_price):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "UPDATE binance_positions SET size = %s, entry_price = %s WHERE position_id = %s;"
    cur.execute(query, (position_id, size, entry_price))

    conn.commit()
    cur.close()


def get_undone_instructions():
    conn = get_db_connection()
    cur = conn.cursor()

    query = """SELECT * FROM instructions;"""
    cur.execute(query)
    instructions = cur.fetchall()

    cur.close()
    return instructions


def mark_instruction_as_done(position_id):
    conn = get_db_connection()
    cur = conn.cursor()

    query = "UPDATE instructions SET is_done = TRUE WHERE id = %s AND is_done = %s;"
    cur.execute(query, (position_id, False))
    conn.commit()

    cur.close()


def create_open_trader_log(trader, symbol, direction, entry_price):
    log_title = 'Засечен е нов сигнал!'

    parts = [
        f'Валута: **{symbol}**\n',
        f'Посока: {'**Short** :red_circle:' if direction == 'Short' else '**Long** :green_circle:'}\n',
        'Leverage: **20x**\n',
        f'Entry price: **${entry_price}**',
    ]

    timestamp = datetime.now()

    conn = get_db_connection()
    cur = conn.cursor()
    query = """INSERT INTO traders (trader, title, timestamp, data)
                   VALUES (%s, %s, %s, %s);"""

    cur.execute(query, (trader, log_title, timestamp, ''.join(parts)))
    conn.commit()
    conn.close()
    cur.close()


def create_add_take_trader_log(trader, symbol, direction, entry_price, change):
    log_title = 'Засечена е промяна в инвестиция!'

    mark_price = get_market_price(symbol=symbol)
    parts = [
        f'Позиция: **{symbol}** - {'**Short** :red_circle:' if direction == 'Short' else 'Long :green_circle:'}\n',
        f'Ново Entry: **${entry_price}**\n' if change > 0 else '',
        f'Текуща цена: **${mark_price}**\n'
        f'Промяна: **{abs(round(change * 100, 2))}%** {':arrow_up:' if change > 0 else ':arrow_down:'}',
    ]

    timestamp = datetime.now()

    conn = get_db_connection()
    cur = conn.cursor()
    query = """INSERT INTO traders (trader, title, timestamp, data)
                   VALUES (%s, %s, %s, %s);"""

    cur.execute(query, (trader, log_title, timestamp, ''.join(parts)))
    conn.commit()
    conn.close()
    cur.close()


def create_close_trader_log(trader, symbol, direction, entry_price, leverage):
    log_title = 'Затваряне на позиция!'

    close_price = get_market_price(symbol=symbol)
    ROI = (close_price - entry_price) / entry_price * leverage * 100 * (-1 if direction == 'Short' else 1)
    trade_lost = ROI < 0

    parts = [
        f'Валута: **{symbol}**\n',
        f'Посока: {'Short:red_circle:' if direction == 'Short' else 'Long:green_circle:'}\n',
        f'Entry / Closed Price: **{round(entry_price, 5)}** / **{round(close_price, 5)}**\n',
        f'{'**Спечелен трейд** :white_check_mark:' if not trade_lost else '**Изгубен трейд** :x:'} | **{round(ROI, 2)}%** ROI',
    ]
    timestamp = datetime.now()

    conn = get_db_connection()
    cur = conn.cursor()
    query = """INSERT INTO traders (trader, title, timestamp, data)
                       VALUES (%s, %s, %s, %s);"""

    cur.execute(query, (trader, log_title, timestamp, ''.join(parts)))
    conn.commit()
    conn.close()
    cur.close()
