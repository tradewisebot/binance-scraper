import datetime

from pybit import unified_trading


def get_tw_analyst_positions():
    tw_analyst_api_key = 'HGaQnECIA1LLd1WQfC'
    tw_analyst_api_secret = 'U8VirpTPX8U4k3BAyThateMSoPUsn4ZYsWoG'

    session = unified_trading.HTTP(api_key=tw_analyst_api_key, api_secret=tw_analyst_api_secret)
    positions = session.get_positions(category='linear', settleCoin='USDT')['result']['list']

    positions_list = []
    for position in positions:
        positions_list.append({
            'symbol': position['symbol'],
            'leverage': int(position['leverage']),
            'direction': 'Long' if position['side'] == 'Buy' else 'Short',
            'size': float(position['size']),
            'entry_price': float(position['avgPrice']),
            'action_time': datetime.datetime.fromtimestamp(int(position['createdTime']) / 1000),
        })
    return positions_list