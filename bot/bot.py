import logging
import traceback
import json
import requests
from datetime import datetime
import asyncio
import time
import os
import pandas as pd

import telegram
from telegram.constants import ParseMode, ChatAction

import config
import redis_client

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DERIBIT_TRADE_API = "https://www.deribit.com/api/v2/public/get_last_trades_by_currency"
BYBIT_TRADE_API = "https://api-testnet.bybit.com/v5/market/recent-trade"
BYBIT_SYMBOL_API = "https://api-testnet.bybit.com/v5/market/instruments-info"
OKX_TRADE_API = "https://www.okx.com/api/v5/public/option-trades"

redis_client = redis_client.RedisClient()
bot = telegram.Bot(token=config.telegram_token)

directory = os.path.dirname(os.path.realpath(__file__))
deribit_combo = pd.read_csv(f"{directory}/deribit_combo.csv")

async def fetch_deribit_data(currency):
    response = requests.get(DERIBIT_TRADE_API, params={
        "currency": currency,
        "kind": "any",
        "count": 500,
        "sorting": "desc",
    })
    data = response.json()
    trades = data["result"]["trades"]
    for trade in trades:
        id = trade['trade_id']
        if not redis_client.is_trade_member(id):
            """ Parse the trade data and return a dict (trade_id, source, symbol, currency, direction, price, size, iv, index_price, block_trade_id, liquidation, timestamp). The trade data is in the following format:
            {
            "trade_seq":207
            "trade_id":"ETH-22858667"
            "timestamp":1679484388529
            "tick_direction":1
            "price":0.0285
            "mark_price":0.027583
            "iv":89.95
            "instrument_name":"ETH-24MAR23-1800-C"
            "index_price":1792.47
            "direction":"buy"
            "size":2
            "block_trade_id":"ETH-44560"
            "liquidation":"M"
            }
            """
            if "block_trade_id" in trade:
                block_trade_id = trade["block_trade_id"]
                trade = {
                    "trade_id": trade["trade_id"],
                    "source": "deribit",
                    "symbol": trade["instrument_name"],
                    "currency": currency,
                    "direction": trade["direction"],
                    "price": trade["price"],
                    "size": trade["amount"],
                    "iv": trade["iv"] if "iv" in trade else None,
                    "index_price": trade["index_price"],
                    "liquidation": True if "liquidation" in trade else False,
                    "timestamp": trade["timestamp"],
                }
                if not redis_client.is_block_trade_id_member(block_trade_id):
                    redis_client.put_block_trade_id(block_trade_id)
                    redis_client.put_block_trade(trade, block_trade_id)
            elif 'iv' in trade:
                trade = {
                    "trade_id": trade["trade_id"],
                    "source": "deribit",
                    "symbol": trade["instrument_name"],
                    "currency": currency,
                    "direction": trade["direction"],
                    "price": trade["price"],
                    "size": trade["amount"],
                    "iv": trade["iv"],
                    "index_price": trade["index_price"],
                    "liquidation": True if "liquidation" in trade else False,
                    "timestamp": trade["timestamp"],
                }
                redis_client.put_trade(trade, id)

async def fetch_bybit_data(symbol):
    response = requests.get(BYBIT_TRADE_API, params={
        "symbol": symbol,
        "category": "option",
    })
    data = response.json()
    trades = data["result"]["list"]
    for trade in trades:
        id = f"bybit_{trade['execId']}"
        if trade["isBlockTrade"] and not redis_client.is_trade_member(id):
            """ Parse the trade data and return a dict (trade_id, source, symbol, currency, direction, price, size, iv, index_price, timestamp). The trade data is in the following format:
            {
            "symbol": "BTC-24MAR23-26000-P",
            "side": "Sell",
            "size": "0.01",
            "price": "97.2",
            "time": "1679518292229",
            "execId": "1b21d10b-53ad-474d-a0e0-79a31380e35c",
            "isBlockTrade": true
            },
            """
            logger.error(trade)
            trade = {
                "trade_id": trade["execId"],
                "source": "bybit",
                "symbol": trade["symbol"],
                "currency": trade["symbol"].split("-")[0],
                "direction": trade["side"],
                "price": trade["price"],
                "size": trade["size"],
                "iv": None,
                "index_price": None,
                "timestamp": trade["time"],
            }

            redis_client.put_trade(trade, id)

async def fetch_okx_data(currency):
    response = requests.get(OKX_TRADE_API, params={
        "instFamily": f"{currency}-USD",
    })
    data = response.json()
    trades = data["data"]
    for trade in trades:
        id = f"okx_{trade['tradeId']}_{trade['ts']}"
        if not redis_client.is_trade_member(id):
            """ Parse the trade data and return a dict (trade_id, source, symbol, currency, direction, price, size, iv, index_price, timestamp). The trade data is in the following format:
            {"fillVol":"0.65430556640625","fwdPx":"1764.388687312925","indexPx":"1764.08","instFamily":"ETH-USD","instId":"ETH-USD-230331-1900-C","markPx":"0.005667868981589025","optType":"C","px":"0.0055","side":"sell","sz":"259","tradeId":"361","ts":"1679882651706"}
            """
            trade = {
                "trade_id": trade["tradeId"],
                "source": "okx",
                "symbol": trade["instId"],
                "currency": currency,
                "direction": trade["side"],
                "price": trade["px"],
                "size": int(trade["sz"])/100 if currency=="BTC" else int(trade["sz"])/10,
                "iv": None,
                "index_price": trade["indexPx"],
                "timestamp": trade["ts"],
            }

            redis_client.put_trade(trade, id)

async def fetch_bybit_symbol():
    # Get timeout
    timeout = redis_client.get_bybit_symbols_timeout()
    if timeout and int(time.time()) < int(timeout):
        symbols = redis_client.get_array('bybit_symbols')
        return symbols
    else:
        btcResponse = requests.get(BYBIT_SYMBOL_API, params={
            "category": "option",
            "baseCoin": "BTC",
        })
        btcData = btcResponse.json()
        btcSymbolList = btcData["result"]["list"]

        ethResponse = requests.get(BYBIT_SYMBOL_API, params={
            "category": "option",
            "baseCoin": "ETH",
        })
        ethData = ethResponse.json()
        ethSymbolList = ethData["result"]["list"]

        # 将btcSymbolList,ethSymbolList数组里的symbol值取出来
        symbols = [symbol["symbol"] for symbol in btcSymbolList] + [symbol["symbol"] for symbol in ethSymbolList]
        # Save the symbols array in Redis and set a timeout
        redis_client.put_array(symbols, 'bybit_symbols')
        redis_client.set_bybit_symbols_timeout(int(time.time()) + 60*30)

        return symbols

async def fetch_deribit_data_all():
    while True:
        try:
            await fetch_deribit_data("BTC")
            await fetch_deribit_data("ETH")
        except Exception as e:
            logger.error(f"Error1: {e}")
            continue

        await asyncio.sleep(30)


async def fetch_okx_data_all():
    while True:
        try:
            await fetch_okx_data("BTC")
            await fetch_okx_data("ETH")
        except Exception as e:
            logger.error(f"Error2: {e}")
            continue
        await asyncio.sleep(60)

async def fetch_bybit_data_all():
    while True:
        try:
            symbols = await fetch_bybit_symbol()
            for symbol in symbols:
                await fetch_bybit_data(symbol)
                await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error3: {e}")
            continue

        await asyncio.sleep(60)

# Define a function to pop 'trade_queue' data from Redis and if BTC's size>=25 or ETH's size>=250 send it to Telegram group
async def handle_trade_data():
    while True:
        try:
            # Pop data from Redis
            data = redis_client.get_trade()
            if data:
                logger.error(f"Pop data from Redis: {data}")
                # Check if the size is >=25 or >=250
                if data["currency"] == "BTC" and float(data["size"]) >= 25:
                    redis_client.put_item(data, 'bigsize_trade_queue')
                elif data["currency"] == "ETH" and float(data["size"]) >= 250:
                    redis_client.put_item(data, 'bigsize_trade_queue')
        except Exception as e:
            logger.error(f"Error4: {e}")
            continue
        # Wait for 10 second before fetching data again
        await asyncio.sleep(0.1)

async def push_block_trade_to_telegram():
    while True:
        try:
            id = redis_client.get_block_trade_id()
            trades = []
            if id:
                while redis_client.get_block_trade_len(id) > 0:
                    trades.append(redis_client.get_block_trade(id))

            if trades:
                # trade["symbol"]可能是"BTC-28JUN21-40000-C", "BTC-28JUN21-40000-P", "ETH-28JUN21-4000-C", "ETH-28JUN21-4000-P", "ETH-PERPETUAL", "ETH-14APR23"等格式。分解trades数据，得到callOrPut, strike, expiry并重新存入trades数组中
                for trade in trades:
                    if trade["symbol"].split("-")[-1] == "C" or trade["symbol"].split("-")[-1] == "P":
                        trade["callOrPut"] = trade["symbol"].split("-")[-1]
                        trade["strike"] = trade["symbol"].split("-")[-2]
                        trade["expiry"] = trade["symbol"].split("-")[-3]
                    else:
                        trade["callOrPut"] = None
                        trade["strike"] = None
                        trade["expiry"] = None

                # sort trades by strike if strike is not None, else by callOrPut and its value P<C if callOrPut is not none, else by expiry
                trades = sorted(trades, key=lambda x: (x["strike"] is None, x["strike"], x["callOrPut"] is None, x["callOrPut"] == "C", x["callOrPut"] == "P", x["expiry"] is None, x["expiry"]))

                # analyse trades to get legs, contract_type, strike, expiry, size_ratio, side
                legs = len(trades)
                contract_type = "N"
                strike = "N"
                expiry = "N"
                size_ratio = "N"
                size = "N"
                if legs == 1:
                    contract_type = trades[0]["callOrPut"]
                    size_ratio = "1"
                    side = "A" if trades[0]["direction"].upper() == "BUY" else "-A"
                elif legs == 2:
                    # contract_type
                    if trades[0]["callOrPut"] is None or trades[1]["callOrPut"] is None:
                        contract_type = "F"
                    elif trades[0]["callOrPut"] == trades[1]["callOrPut"] == "C":
                        contract_type = "C"
                    elif trades[0]["callOrPut"] == trades[1]["callOrPut"] == "P":
                        contract_type = "P"
                    elif (trades[0]["callOrPut"] == "P" and trades[1]["callOrPut"] == "C") or (trades[0]["callOrPut"] == "C" and trades[1]["callOrPut"] == "P"):
                        contract_type = "PC"
                    # strike
                    if contract_type=="F" or trades[0]["strike"] is None or trades[1]["strike"] is None:
                        strike = "N"
                    elif trades[0]["strike"] < trades[1]["strike"]:
                        strike = "A<B"
                    elif trades[0]["strike"] == trades[1]["strike"]:
                        strike = "A=B"
                    else:
                        strike = "A>B"
                    # expiry
                    if contract_type=="F" or trades[0]["expiry"] is None or trades[1]["expiry"] is None:
                        expiry = "N"
                    elif trades[0]["expiry"] == trades[1]["expiry"]:
                        expiry = "A=B"
                    elif trades[0]["expiry"] < trades[1]["expiry"]:
                        expiry = "A<B"
                    else:
                        expiry = "A>B"
                    # size_ratio
                    if contract_type=="F" or trades[0]["size"] is None or trades[1]["size"] is None:
                        size_ratio = "N"
                    elif trades[0]["size"] == trades[1]["size"]:
                        size_ratio = "1:1"
                    elif trades[0]["size"] < trades[1]["size"]:
                        size_ratio = "1:N"
                    else:
                        size_ratio = "N:1"
                    # side
                    if contract_type=="F":
                        side = "N"
                    else:
                        sideA = "A" if trades[0]["direction"].upper() == "BUY" else "-A"
                        sideB = "+B" if trades[1]["direction"].upper() == "BUY" else "-B"
                        side = sideA + sideB
                elif legs == 3:
                    # contract_type
                    if trades[0]["callOrPut"] == trades[1]["callOrPut"] == trades[2]["callOrPut"] == "C":
                        contract_type = "C"
                    elif trades[0]["callOrPut"] == trades[1]["callOrPut"] == trades[2]["callOrPut"] == "P":
                        contract_type = "P"
                    # strike
                    if trades[0]["strike"] is None or trades[1]["strike"] is None or trades[2]["strike"] is None:
                        strike = "N"
                    elif trades[0]["strike"] < trades[1]["strike"] < trades[2]["strike"]:
                        strike = "A<B<C"
                    elif trades[0]["strike"] == trades[1]["strike"] < trades[2]["strike"]:
                        strike = "A=B<C"
                    elif trades[0]["strike"] < trades[1]["strike"] == trades[2]["strike"]:
                        strike = "A<B=C"
                    elif trades[0]["strike"] == trades[1]["strike"] == trades[2]["strike"]:
                        strike = "A=B=C"
                    # expiry
                    if trades[0]["expiry"] is None or trades[1]["expiry"] is None or trades[2]["expiry"] is None:
                        expiry = "N"
                    elif trades[0]["expiry"] == trades[1]["expiry"] == trades[2]["expiry"]:
                        expiry = "A=B=C"
                    elif trades[0]["expiry"] < trades[1]["expiry"] < trades[2]["expiry"]:
                        expiry = "A<B<C"
                    elif trades[0]["expiry"] == trades[1]["expiry"] < trades[2]["expiry"]:
                        expiry = "A=B<C"
                    elif trades[0]["expiry"] < trades[1]["expiry"] == trades[2]["expiry"]:
                        expiry = "A<B=C"
                    elif trades[0]["expiry"] < trades[1]["expiry"] > trades[2]["expiry"]:
                        expiry = "A>B>C"
                    # size_ratio: "1:2:1" or None
                    if trades[0]["size"]*2 == trades[1]["size"] == trades[2]["size"]*2:
                        size_ratio = "1:2:1"
                    # side
                    sideA = "A" if trades[0]["direction"].upper() == "BUY" else "-A"
                    sideB = "+B" if trades[1]["direction"].upper() == "BUY" else "-B"
                    sideC = "+C" if trades[2]["direction"].upper() == "BUY" else "-C"
                    side = sideA + sideB + sideC
                elif legs == 4:
                    # contract_type: "C", "P", "PPCC", None
                    if trades[0]["callOrPut"] == trades[1]["callOrPut"] == trades[2]["callOrPut"] == trades[3]["callOrPut"] == "C":
                        contract_type = "C"
                    elif trades[0]["callOrPut"] == trades[1]["callOrPut"] == trades[2]["callOrPut"] == trades[3]["callOrPut"] == "P":
                        contract_type = "P"
                    elif trades[0]["callOrPut"] == trades[1]["callOrPut"] == "P" and trades[2]["callOrPut"] == trades[3]["callOrPut"] == "C":
                        contract_type = "PPCC"
                    # strike: "A<B<C<D", "A<B=C<D" or None
                    if trades[0]["strike"] is None or trades[1]["strike"] is None or trades[2]["strike"] is None or trades[3]["strike"] is None:
                        strike = "N"
                    elif trades[0]["strike"] < trades[1]["strike"] < trades[2]["strike"] < trades[3]["strike"]:
                        strike = "A<B<C<D"
                    elif trades[0]["strike"] < trades[1]["strike"] == trades[2]["strike"] < trades[3]["strike"]:
                        strike = "A<B=C<D"
                    # expiry: "A=B=C=D" or None
                    if trades[0]["expiry"] == trades[1]["expiry"] == trades[2]["expiry"] == trades[3]["expiry"]:
                        expiry = "A=B=C=D"
                    # size_ratio: "1:1:1:1" or None
                    if trades[0]["size"] == trades[1]["size"] == trades[2]["size"] == trades[3]["size"]:
                        size_ratio = "1:1:1:1"
                        # side
                    sideA = "A" if trades[0]["direction"].upper() == "BUY" else "-A"
                    sideB = "+B" if trades[1]["direction"].upper() == "BUY" else "-B"
                    sideC = "+C" if trades[2]["direction"].upper() == "BUY" else "-C"
                    sideD = "+D" if trades[3]["direction"].upper() == "BUY" else "-D"
                    side = sideA + sideB + sideC + sideD

                # 根据参数查询策略名称和视图
                result = deribit_combo.loc[(deribit_combo["Legs"]==legs) &
                                (deribit_combo["Contract Type"]==contract_type) &
                                (deribit_combo["Strike"]==strike) &
                                (deribit_combo["Expiry"]==expiry) &
                                (deribit_combo["Size Ratio"]==size_ratio) &
                                (deribit_combo["Side"]==side)]

                # 输出结果
                if result.empty:
                    text = "CUSTOM STRATEGY"
                else:
                    view = result["View"].values[0]
                    if not pd.isna(view):
                        text = f'{result["Strategy Name"].values[0]} ({view})'
                    else:
                        text = result["Strategy Name"].values[0]

                text += '\n'
                text += f"<b><i>📊 DERIBIT {id.decode('utf-8')}</i></b>"
                text += '\n'
                text += f'<i>🕛 {datetime.fromtimestamp(int(trades[0]["timestamp"])//1000)} UTC</i>'

                for trade in trades:
                    direction = trade["direction"].upper()
                    callOrPut = trade["symbol"].split("-")[-1]
                    currency = trade["currency"]
                    if callOrPut == "C" or callOrPut == "P":
                        text += '\n'
                        text += f'{"🔴" if direction=="SELL" else "🟢"} {direction} '
                        text += f'{"🔶" if currency=="BTC" else "🔷"} {trade["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
                        text += f'at {trade["price"]} {"U" if trade["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${trade["price"] if trade["source"].upper()=="BYBIT" else float(trade["price"])*float(trade["index_price"]):,.2f}) '
                        text += f'<b>Size</b>: {trade["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(trade["size"])*float(trade["index_price"])/1000:,.2f}K){" ‼️‼️" if (trade["currency"] == "BTC" and float(trade["size"]) >= 1000) or (trade["currency"] == "ETH" and float(trade["size"]) >= 10000) else ""} '
                        text += f'<b>IV</b>: {str(trade["iv"])+"%"} '
                        text += f'<b>Index Price</b>: {"$"+str(trade["index_price"])}'
                    else:
                        text += '\n'
                        text += f'{"🔴" if direction=="SELL" else "🟢"} {direction} '
                        text += f'{"🔶" if currency=="BTC" else "🔷"} {trade["symbol"]} '
                        text += f'at ${float(trade["price"]):,.2f} '
                        text += f'<b>Size</b>: {float(trade["size"]) /1000:,.2f}K '
                        text += f'<b>Index Price</b>: {"$"+str(trade["index_price"])}'
                text += '\n'
                text += f'<i>#block</i>'

                # Send the text to Telegram group
                await bot.send_message(
                    chat_id=config.group_chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
        except Exception as e:
            logger.error(f"Error5: {e}")
            continue
        # Wait for 10 second before fetching data again
        await asyncio.sleep(5)

# Define a function to send the data with prettify format to Telegram group
async def push_trade_to_telegram():
    while True:
        try:
            # Pop data from Redis
            data = redis_client.get_item('bigsize_trade_queue')
            if data:
                direction = data["direction"].upper()
                callOrPut = data["symbol"].split("-")[-1]
                currency = data["currency"]
                # 根据direction和callOrPut判断strategy是"LONG CALL","SHORT CALL","LONG PUT"还是"SHORT PUT"
                if direction == "BUY":
                    if callOrPut == "C":
                        strategy = "LONG CALL"
                    elif callOrPut == "P":
                        strategy = "LONG PUT"
                elif direction == "SELL":
                    if callOrPut == "C":
                        strategy = "SHORT CALL"
                    elif callOrPut == "P":
                        strategy = "SHORT PUT"

                text = strategy
                text += '\n'
                text += f'<b><i>📊 {data["source"].upper()} {data["trade_id"]}</i></b>'
                text += '\n'
                text += f'<i>🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC</i>'
                text += '\n'
                text += f'{"🔴" if direction=="SELL" else "🟢"} {direction} '
                text += f'{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
                text += f'at {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) '
                text += f'<b>Size</b>: {data["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(data["size"])*float(data["index_price"])/1000:,.2f}K){" ‼️‼️" if (data["currency"] == "BTC" and float(data["size"]) >= 1000) or (data["currency"] == "ETH" and float(data["size"]) >= 10000) else ""} '
                text += f'<b>IV</b>: {str(data["iv"])+"%" if data["iv"] else "Unknown"} '
                text += f'<b>Index Price</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}'
                text += '\n'
                if "liquidation" in data and data["liquidation"]:
                    text += f'<i>#liquidation</i>'
                else:
                    text += f'<i>#onscreen</i>'

                # Send the data to Telegram group
                await bot.send_message(
                    chat_id=config.group_chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
        except Exception as e:
            logger.error(f"Error6: {e}")
            continue
        # Wait for 5 second before fetching data again
        await asyncio.sleep(5)

def run_bot() -> None:
    # Create two threads to fetch block trade data and send it to Telegram group by using asyncio
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(fetch_deribit_data_all())
        loop.create_task(fetch_okx_data_all())
        loop.create_task(fetch_bybit_data_all())
        loop.create_task(handle_trade_data())
        loop.create_task(push_trade_to_telegram())
        loop.create_task(push_block_trade_to_telegram())
        loop.run_forever()
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    run_bot()
    
