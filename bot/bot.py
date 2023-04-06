import logging
import traceback
import json
import requests
from datetime import datetime
import asyncio
import time

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
        id = f"deribit_{trade['block_trade_id'] if 'block_trade_id' in trade else trade['trade_id']}"
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
                trade = {
                    "trade_id": trade["block_trade_id"] if "block_trade_id" in trade else trade["trade_id"],
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
                if not redis_client.is_block_trade_member(id):
                    redis_client.put_block_trade_id(id)
                redis_client.put_block_trade(trade, id)
            elif 'iv' in trade:
                trade = {
                    "trade_id": trade["block_trade_id"] if "block_trade_id" in trade else trade["trade_id"],
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
            logger.error(f"Error: {e}")
            continue

        await asyncio.sleep(30)


async def fetch_okx_data_all():
    while True:
        try:
            await fetch_okx_data("BTC")
            await fetch_okx_data("ETH")
        except Exception as e:
            logger.error(f"Error: {e}")
            continue
        await asyncio.sleep(60)

async def fetch_bybit_data_all():
    while True:
        try:
            symbols = await fetch_bybit_symbol()
            for symbol in symbols:
                await fetch_bybit_data(symbol)
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error: {e}")
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
                    redis_client.put_item(data, 'trade_queue')
                elif data["currency"] == "ETH" and float(data["size"]) >= 250:
                    redis_client.put_item(data, 'trade_queue')
        except Exception as e:
            logger.error(f"Error: {e}")
            continue
        # Wait for 10 second before fetching data again
        await asyncio.sleep(0.1)

async def push_block_trade_to_telegram():
    while True:
        try:
            # Pop data from Redis
            id = redis_client.get_block_trade_id()
            if id:
                text = "<b><i>🔔Block Trade🔔️ 📊 DERIBIT\n</i></b>"
                while redis_client.get_block_trade_len(id) > 0:
                    blockTrade = redis_client.get_block_trade(id)
                    if blockTrade:
                        direction = data["direction"].upper()
                        callOrPut = data["symbol"].split("-")[-1]
                        currency = data["currency"]
                        if callOrPut == "C" or callOrPut == "P":
                            if (data["currency"] == "BTC" and float(data["size"]) >= 1000) or (data["currency"] == "ETH" and float(data["size"]) >= 10000):
                                text += f'<i>‼️‼️🔔🔔🔔🔔🔔🔔‼️‼️ <b>🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC <b>{"🔴" if direction=="SELL" else "🟢"} {direction}\n{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"}</b> <b>Price</b>: {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) <b>Size</b>: {data["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(data["size"])*float(data["index_price"])/1000:,.2f}K) ‼️‼️‼️ <b>IV</b>: {str(data["iv"])+"%" if data["iv"] else "Unknown"} <b>Index Price</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}</b></i>'
                            else:
                                text += f'<i>🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC <b>{"🔴" if direction=="SELL" else "🟢"} {direction} {"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"}</b> <b>Price</b>: {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) <b>Size</b>: {data["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(data["size"])*float(data["index_price"])/1000:,.2f}K) <b>IV</b>: {str(data["iv"])+"%" if data["iv"] else "Unknown"} <b>Index Price</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}</i>'
                        else:
                            if (data["currency"] == "BTC" and float(data["size"]) >= 1000) or (data["currency"] == "ETH" and float(data["size"]) >= 10000):
                                text += f'<i>‼️‼️🔔🔔🔔🔔🔔🔔‼️‼️ <b>🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC <b>{"🔴" if direction=="SELL" else "🟢"} {direction}\n{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]}</b> <b>Price</b>: {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) <b>Size</b>: {data["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(data["size"])*float(data["index_price"])/1000:,.2f}K) ‼️‼️‼️ <b>Index Price</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}</b></i>'
                            else:
                                text += f'<i>🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC <b>{"🔴" if direction=="SELL" else "🟢"} {direction} {"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} <b>Price</b>: {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) <b>Size</b>: {data["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(data["size"])*float(data["index_price"])/1000:,.2f}K) <b>Index Price</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}</i>'
                    await asyncio.sleep(0.1)

                text += f'\n<i><b>Trade Id: {data["trade_id"]}</b>\n#block</i>'

                # Send the data to Telegram group
                await bot.send_message(
                    chat_id=config.group_chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
        except Exception as e:
            logger.error(f"Error: {e}")
            continue
        # Wait for 10 second before fetching data again
        await asyncio.sleep(5)

# Define a function to send the data with prettify format to Telegram group
async def push_trade_to_telegram():
    while True:
        try:
            # Pop data from Redis
            data = redis_client.get_item('trade_queue')
            if data:
                direction = data["direction"].upper()
                callOrPut = data["symbol"].split("-")[-1]
                currency = data["currency"]
                if (data["currency"] == "BTC" and float(data["size"]) >= 1000) or (data["currency"] == "ETH" and float(data["size"]) >= 10000):
                    text = f'<i>‼️‼️🔔🔔🔔🔔🔔🔔‼️‼️\n<b>📊 {data["source"].upper()}\n🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC\n<b>{"🔴" if direction=="SELL" else "🟢"} {direction}\n{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"}</b>\n<b>Price</b>: {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f})\n<b>Size</b>: {data["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(data["size"])*float(data["index_price"])/1000:,.2f}K) ‼️‼️‼️\n<b>IV</b>: {str(data["iv"])+"%" if data["iv"] else "Unknown"}\n<b>Index Price</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}\n<b>Trade Id</b>: {data["trade_id"]}</b></i>'
                else:
                    text = f'<i>📊 {data["source"].upper()}\n🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC\n<b>{"🔴" if direction=="SELL" else "🟢"} {direction}\n{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"}</b>\n<b>Price</b>: {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f})\n<b>Size</b>: {data["size"]} {"₿" if currency=="BTC" else "Ξ"} (${float(data["size"])*float(data["index_price"])/1000:,.2f}K)\n<b>IV</b>: {str(data["iv"])+"%" if data["iv"] else "Unknown"}\n<b>Index Price</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}\n<b>Trade Id</b>: {data["trade_id"]}</i>'

                if data["liquidation"]:
                    text += f'\n<i>#liquidation</i>'
                else:
                    text += f'\n<i>#onscreen</i>'

                # Send the data to Telegram group
                await bot.send_message(
                    chat_id=config.group_chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
        except Exception as e:
            logger.error(f"Error: {e}")
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
    
