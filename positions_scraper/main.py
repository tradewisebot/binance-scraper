from playwright.sync_api import Playwright, sync_playwright, expect
from datetime import datetime
import time

from bybit_functions import get_tw_analyst_positions
from db_scripts import save_to_db, get_all_traders, position_exists, set_row_open, set_open_field, \
    get_safe_db_connection, get_trader_positions, delete_trader_positions, get_position, get_db_connection, \
    get_position_change, create_instruction, get_closed_positions, create_open_trader_log, create_add_take_trader_log, \
    create_close_trader_log, update_position_db

# from pickle_playground import force_close_user_positions

# When trader takes off some amount of investment - size changes
# When trader adds some more to investment - size, entry_price, timestamp change


traders = {
    1: 'u5m4d',
    2: 'ngekrd',
    3: 'jvs8y',
    4: 'edcd5',
    5: 'wgtx5',
    6: '8uhzp',
    7: 'zxeuq',
    8: 'yf1e6',
    9: 'tw-analyst',
}


def initialize_browser_context():
    browser = playwright.firefox.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    return browser, context, page


def login_into_binance(page):
    page.goto("https://www.binance.com")

    not_found_container = page.query_selector(".not-fount-container")
    if not_found_container:
        raise ConnectionError('Binance website is down!')

    page.get_by_role("button", name="Accept Cookies & Continue").click()
    page.frame_locator("iframe[title=\"Диалогов прозорец за функцията „Вход с Google“\"]").get_by_label(
        'Затваряне').click()
    page.get_by_role("banner").get_by_role("link", name="Log In").click()
    time.sleep(9)
    with page.expect_popup() as page1_info:
        page.frame_locator("iframe[title=\"Бутон за функцията „Вход с Google“\"]").get_by_label("Вход с Google").click()
    page1_info.value.locator('#identifierId').fill('tradewisebot@gmail.com')
    page1_info.value.click('#identifierNext')
    page1_info.value.locator("input[name=\"Passwd\"]").fill('kurzalevski69')
    page1_info.value.click('#passwordNext')

    page.wait_for_selector('.bn-button.bn-button__primary.data-size-small.deposit-btn')

    page1 = page1_info.value
    page1.close()


def visit_trader_profile(context, trader_profile_url, trader_id):
    # Open a new tab
    page2 = context.new_page()
    page2.goto(trader_profile_url)

    # page2.wait_for_selector('.bn-table-tbody')
    time.sleep(5)
    table = page2.locator("tbody[class=\"bn-table-tbody\"]")
    cells = table.locator('.bn-table-cell').all()
    positions = []
    if len(cells) > 10:
        for i in range(0, len(cells), 7):
            chunk = [cell.inner_text() for cell in cells[i:i + 7]]
            sdl = chunk[0].split('\n')

            symbol_data = sdl[0].split(' ')
            if symbol_data[1] != 'Perpetual' or 'USDT' not in symbol_data[0]:
                continue

            symbol, direction = symbol_data[0], sdl[1]
            leverage = float(''.join(c for c in sdl[2] if c.isdigit()))
            size, entry_price = float(chunk[1]), float(chunk[2].replace(',', ''))
            trade_time = datetime.strptime(chunk[5], '%Y-%m-%d %H:%M:%S')

            data = (
                traders[trader_id],
                symbol,
                direction,
                leverage,
                size,
                entry_price,
                trade_time,
                True,
            )
            positions.append(data)

    page2.close()
    return positions


def run(pw: Playwright) -> None:
    conn = get_db_connection()
    set_open_field(conn)
    browser, context, page = initialize_browser_context()
    login_into_binance(page)

    # Check Binance traders for positions and updates in positions
    positions = []
    for trader in get_all_traders(conn):
        positions.extend(
            visit_trader_profile(
                context,
                trader.url,
                trader.trader_id,
            )
        )

    # Check tw-analyst for positions
    for position in get_tw_analyst_positions():
        data = (
            'tw-analyst',
            position['symbol'],
            position['direction'],
            position['leverage'],
            position['size'],
            position['entry_price'],
            position['action_time'],
            True,
        )
        positions.append(data)

    for position in positions:
        trader = position[0]
        symbol = position[1]
        direction = position[2]
        size = position[4]
        entry_price = position[5]

        check_data = (trader, symbol, direction)
        position_id = position_exists(check_data, conn)
        if position_id == -1:
            save_to_db(position, conn)
            create_open_trader_log(trader, symbol, direction, entry_price)
            # Creates an open instruction
            open_instruction_data = (trader, symbol, direction, 'Open', None, size, entry_price, False)
            create_instruction(open_instruction_data)
        else:
            position_changes = get_position_change(position_id, size, entry_price, conn)
            size_change = position_changes['size_change']
            if size_change != 0:
                update_position_db(position_id, size, entry_price)
                create_add_take_trader_log(trader, symbol, direction, entry_price, size_change)
                is_up_change = size_change > 0
                if is_up_change:
                    # Creates an add instruction
                    add_instruction_data = (trader, symbol, direction, 'Add', size_change, size, entry_price, False)
                    create_instruction(add_instruction_data)
                else:
                    # Creates a take instruction
                    add_instruction_data = (trader, symbol, direction, 'Take', size_change, size, entry_price, False)
                    create_instruction(add_instruction_data)

            set_row_open(position_id, conn)

    closed_positions = get_closed_positions(conn)
    print(closed_positions)
    for position in closed_positions:
        trader = position[1]
        symbol = position[2]
        direction = position[3]

        # Creates a closed trader log
        create_close_trader_log(trader, symbol, direction, position[6], position[4])

        # Creates a close instruction
        close_instruction_data = (trader, symbol, direction, 'Close', None, position[4], position[5], False)
        create_instruction(close_instruction_data)


with sync_playwright() as playwright:
    st = time.time()
    run(playwright)
    et = time.time()
    print(et - st)
