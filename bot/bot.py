import logging
import requests
import asyncio
import time
from datetime import datetime
import os
import pandas as pd

import telegram
from telegram.constants import ParseMode

import config
import redis_client
import paradigm

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DERIBIT_TRADE_API = "https://www.deribit.com/api/v2/public/get_last_trades_by_currency"
DERIBIT_TICKER_API = "https://www.deribit.com/api/v2/public/ticker"
BYBIT_TRADE_API = "https://api-testnet.bybit.com/v5/market/recent-trade"
BYBIT_SYMBOL_API = "https://api-testnet.bybit.com/v5/market/instruments-info"
OKX_TRADE_API = "https://www.okx.com/api/v5/public/option-trades"
SIGNALPLUS_PUSH_TRADE_API = "https://mizar-gateway.signalplus.com/mizar/block_trades/save"

redis_client = redis_client.RedisClient()
bot = telegram.Bot(token=config.telegram_token)
paradigm = paradigm.Paradigm(access_key=config.paradigm_access_key, secret_key=config.paradigm_secret_key)

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
    # sort trades in ascending order
    trades.sort(key=lambda x: x["trade_seq"])
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
                # next trade if iv is none and size is less than 500K
                if "iv" not in trade and float(trade["amount"]) < 500000:
                    continue
                block_trade_id = trade["block_trade_id"]
                # get greeks if iv in trade
                if "iv" in trade:
                    ticker = requests.get(DERIBIT_TICKER_API, params={
                        "instrument_name": trade["instrument_name"],
                    }).json()
                    greeks = ticker["result"]["greeks"]
                    oi_stored = redis_client.get_data(f'oi_{trade["instrument_name"]}')
                    trade = {
                        "trade_id": trade["trade_id"],
                        "block_trade_id": block_trade_id,
                        "source": "deribit",
                        "symbol": trade["instrument_name"],
                        "currency": currency,
                        "direction": trade["direction"],
                        "price": trade["price"],
                        "size": trade["amount"],
                        "iv": trade["iv"],
                        "greeks": greeks,
                        "bid": ticker["result"]["best_bid_price"],
                        "bid_amount": ticker["result"]["best_bid_amount"],
                        "ask": ticker["result"]["best_ask_price"],
                        "ask_amount": ticker["result"]["best_ask_amount"],
                        "mark": ticker["result"]["mark_price"],
                        "oi_change": float(ticker["result"]["open_interest"]) - float(oi_stored) if oi_stored is not None else 0,
                        "index_price": trade["index_price"],
                        "liquidation": True if "liquidation" in trade else False,
                        "timestamp": trade["timestamp"],
                    }
                    redis_client.set_data(f'oi_{trade["symbol"]}', ticker["result"]["open_interest"])
                else:
                    trade = {
                        "trade_id": trade["trade_id"],
                        "block_trade_id": block_trade_id,
                        "source": "deribit",
                        "symbol": trade["instrument_name"],
                        "currency": currency,
                        "direction": trade["direction"],
                        "price": trade["price"],
                        "size": trade["amount"],
                        "iv": None,
                        "oi_change": 0,
                        "index_price": trade["index_price"],
                        "liquidation": True if "liquidation" in trade else False,
                        "timestamp": trade["timestamp"],
                    }
                if not redis_client.is_block_trade_id_member(block_trade_id):
                    redis_client.put_block_trade_id(block_trade_id)
                redis_client.put_block_trade(trade, block_trade_id)

                # # midas only
                # if ((trade["currency"] == "BTC" and float(trade["size"]) >= 500) or (trade["currency"] == "ETH" and float(trade["size"]) >= 1000)):
                #     if not redis_client.is_block_trade_id_member(f"midas_{block_trade_id}"):
                #         redis_client.put_block_trade_id(f"midas_{block_trade_id}")
                #     redis_client.put_block_trade(trade, f"midas_{block_trade_id}")
                # # signalplus only
                # #if ((trade["currency"] == "BTC" and float(trade["size"]) >= 500) or (trade["currency"] == "ETH" and float(trade["size"]) >= 2000)) and trade["iv"] is not None:
                # if ((trade["currency"] == "BTC" and float(trade["size"]) >= 500) or (trade["currency"] == "ETH" and float(trade["size"]) >= 5000)):
                #     if not redis_client.is_block_trade_id_member(f"signalplus_{block_trade_id}"):
                #         redis_client.put_block_trade_id(f"signalplus_{block_trade_id}")
                #     redis_client.put_block_trade(trade, f"signalplus_{block_trade_id}")
                # # playground only
                # if ((trade["currency"] == "BTC" and float(trade["size"]) >= 1000) or (trade["currency"] == "ETH" and float(trade["size"]) >= 10000)):
                #     if not redis_client.is_block_trade_id_member(f"playground_{block_trade_id}"):
                #         redis_client.put_block_trade_id(f"playground_{block_trade_id}")
                #     redis_client.put_block_trade(trade, f"playground_{block_trade_id}")
                # # breavan horward only
                # if ((trade["currency"] == "BTC" and float(trade["size"]) >= 49) or (trade["currency"] == "ETH" and float(trade["size"]) >= 999)):
                #     if not redis_client.is_block_trade_id_member(f"breavan_{block_trade_id}"):
                #         redis_client.put_block_trade_id(f"breavan_{block_trade_id}")
                #     redis_client.put_block_trade(trade, f"breavan_{block_trade_id}")
                # # fbg only
                # if ((trade["currency"] == "BTC" and float(trade["size"]) >= 100) or (trade["currency"] == "ETH" and float(trade["size"]) >= 1000)):
                #     if not redis_client.is_block_trade_id_member(f"fbg_{block_trade_id}"):
                #         redis_client.put_block_trade_id(f"fbg_{block_trade_id}")
                #     redis_client.put_block_trade(trade, f"fbg_{block_trade_id}")
                # # galaxy only
                # if ((trade["currency"] == "BTC" and float(trade["size"]) >= 25) or (trade["currency"] == "ETH" and float(trade["size"]) >= 250)):
                #     if not redis_client.is_block_trade_id_member(f"galaxy_{block_trade_id}"):
                #         redis_client.put_block_trade_id(f"galaxy_{block_trade_id}")
                #     redis_client.put_block_trade(trade, f"galaxy_{block_trade_id}")
                # # astron only
                # if ((trade["currency"] == "BTC" and float(trade["size"]) >= 500) or (trade["currency"] == "ETH" and float(trade["size"]) >= 5000)):
                #     if not redis_client.is_block_trade_id_member(f"astron_{block_trade_id}"):
                #         redis_client.put_block_trade_id(f"astron_{block_trade_id}")
                #     redis_client.put_block_trade(trade, f"astron_{block_trade_id}")

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
                ticker = requests.get(DERIBIT_TICKER_API, params={
                    "instrument_name": trade["symbol"],
                }).json()
                oi_stored = redis_client.get_data(f'oi_{trade["symbol"]}')
                trade["greeks"] = ticker["result"]["greeks"]
                trade["bid"] = ticker["result"]["best_bid_price"]
                trade["bid_amount"] = ticker["result"]["best_bid_amount"]
                trade["ask"] = ticker["result"]["best_ask_price"]
                trade["ask_amount"] = ticker["result"]["best_ask_amount"]
                trade["mark"] = ticker["result"]["mark_price"]
                trade["oi_change"] = float(ticker["result"]["open_interest"]) - float(oi_stored) if oi_stored is not None else 0
                redis_client.set_data(f'oi_{trade["symbol"]}', ticker["result"]["open_interest"])
                redis_client.put_trade(trade, id)

async def fetch_bybit_data(symbol):
    response = requests.get(BYBIT_TRADE_API, params={
        "symbol": symbol,
        "category": "option",
    })
    data = response.json()
    if data["retCode"] != 0:
        logger.error(f"Error fetching bybit data for {symbol}.")
        return
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
                "oi_change": 0,
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
            {"fillVol":"0.65430556640625","fwdPx":"1764.388687312925","idxPx":"1764.08","instFamily":"ETH-USD","instId":"ETH-USD-230331-1900-C","markPx":"0.005667868981589025","optType":"C","px":"0.0055","side":"sell","sz":"259","tradeId":"361","ts":"1679882651706"}
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
                "oi_change": 0,
                "index_price": trade["idxPx"],
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

# fetch paradigm trade timestamp
async def fetch_paradigm_trade_timestamp():
    while True:
        try:
            await fetch_paradigm_grfq_timestamp()
            await fetch_paradigm_drfq_timestamp()
            # clear the expired timestamp
            redis_client.remove_paradigm_trade_timestamp()
        except Exception as e:
            logger.error(f"Paradigm Error: {e}")
            continue

        await asyncio.sleep(10)

async def fetch_paradigm_grfq_timestamp():
    trades = paradigm.get_trade_tape('/v1/grfq/trades', 'GET', '')
    """Parse the trades data and save traded in redis set. The trades data is in the following format: {"count":32576,"next":"cD0yMDIzLTA0LTE5KzA2JTNBMDglM0EwNi40MTIxMzMlMkIwMCUzQTAw","results":[{"action":"BUY","id":50033336,"description":"Put  26 May 23  26000","instrument_kind":"OPTION","mark_price":"0.0238","price":"0.0244","product_codes":["DO"],"quantity":"25","quote_currency":"BTC","rfq_id":50043681,"traded":1681900075018.337,"venue":"DBT"},{"action":"BUY","id":50033335,"description":"Put  26 May 23  26000","instrument_kind":"OPTION","mark_price":"0.0238","price":"0.0244","product_codes":["DO"],"quantity":"25","quote_currency":"BTC","rfq_id":50043681,"traded":1681900074997.7478,"venue":"DBT"}]}"""
    for trade in trades["results"]:
        timestamp = int(trade["traded"])
        redis_client.add_paradigm_trade_timestamp(timestamp)

async def fetch_paradigm_drfq_timestamp():
    trades = paradigm.get_trade_tape('/v2/drfq/trade_tape', 'GET', '')
    """Parse the trades data and save traded in redis set. The trades data is in the following format:{"count":2028,"next":"cD0yMDIzLTA0LTE4KzE0JTNBNDElM0EzOC41MjQ3MDYlMkIwMCUzQTAw","results":[{"id":"bt_2OdZ0MtOcOw21bJDstIaufMlkE1","rfq_id":"r_2OdYmXkbFpkc3ZRrk1B9ADDSsMm","venue":"DBT","kind":"OPTION","state":"FILLED","executed_at":1681892195920.0461,"filled_at":1681892196000.0,"side":"BUY","price":"-0.0126","quantity":"20","legs":[{"instrument_id":222841,"instrument_name":"BTC-28APR23-30000-P","price":"0.0383","product_code":"DO","quantity":"20","ratio":"1","side":"SELL"},{"instrument_id":222840,"instrument_name":"BTC-28APR23-30000-C","price":"0.0229","product_code":"DO","quantity":"20","ratio":"1","side":"SELL"},{"instrument_id":234763,"instrument_name":"BTC-26MAY23-31000-C","price":"0.0486","product_code":"DO","quantity":"20","ratio":"1","side":"BUY"}],"strategy_description":"DO_BTC-28APR23-30000-P_BTC-28APR23-30000-C_BTC-26MAY23-31000-C","description":"Cstm  -1.00  Put  28 Apr 23  30000\n      -1.00  Call  28 Apr 23  30000\n      +1.00  Call  26 May 23  31000","quote_currency":"BTC","mark_price":"-0.0139"},{"id":"bt_2OdYXDGE7WF0Yww82iSsLIK0Y9u","rfq_id":"r_2OdYQQVH6J39Pk0jLHmwadm9sMg","venue":"DBT","kind":"OPTION","state":"FILLED","executed_at":1681891963639.525,"filled_at":1681891963000.0,"side":"BUY","price":"0.0319","quantity":"20","legs":[{"instrument_id":222842,"instrument_name":"BTC-28APR23-32000-C","price":"0.006","product_code":"DO","quantity":"20","ratio":"1","side":"SELL"},{"instrument_id":229778,"instrument_name":"BTC-26MAY23-32000-C","price":"0.0379","product_code":"DO","quantity":"20","ratio":"1","side":"BUY"}],"strategy_description":"DO_BTC-28APR23-32000-C_BTC-26MAY23-32000-C","description":"CCal  28 Apr 23 32000 / 26 May 23 32000","quote_currency":"BTC","mark_price":"0.0305"}]}"""
    for trade in trades["results"]:
        timestamp = int(trade["filled_at"])
        redis_client.add_paradigm_trade_timestamp(timestamp)

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
                # if data["price"] <= 0.0005 continue the loop
                if float(data["price"]) <= 0.0005:
                    continue

                # logger.error(f"Pop data from Redis: {data}")
                # Check if the size is >=25 or >=250
                if data["currency"] == "BTC" and float(data["size"]) >= 25:
                    redis_client.put_item(data, 'bigsize_trade_queue')
                    # galaxy only
                    redis_client.put_item(data, 'galaxy_trade_queue')
                    # breavan horward only
                    if float(data["size"]) >= 49:
                        redis_client.put_item(data, 'breavan_trade_queue')
                    # fbg only
                    if float(data["size"]) >= 100:
                        redis_client.put_item(data, 'fbg_trade_queue')
                    # midas only
                    if float(data["size"]) >= 500:
                        redis_client.put_item(data, 'midas_trade_queue')
                        redis_client.put_item(data, 'astron_trade_queue')
                        # signalplus
                        redis_client.put_item(data, 'signalplus_trade_queue')
                        # playground
                        if float(data["size"]) >= 1000:
                            redis_client.put_item(data, 'playground_trade_queue')
                elif data["currency"] == "ETH" and float(data["size"]) >= 250:
                    redis_client.put_item(data, 'bigsize_trade_queue')
                    # galaxy only
                    redis_client.put_item(data, 'galaxy_trade_queue')
                    # breavan horward only
                    if float(data["size"]) >= 999:
                        redis_client.put_item(data, 'breavan_trade_queue')
                    # midas only
                    if float(data["size"]) >= 1000:
                        redis_client.put_item(data, 'midas_trade_queue')
                        redis_client.put_item(data, 'fbg_trade_queue')
                        # signalplus
                        if float(data["size"]) >= 2000:
                            # redis_client.put_item(data, 'signalplus_trade_queue')
                            if float(data["size"]) >= 5000:
                                redis_client.put_item(data, 'astron_trade_queue')
                                redis_client.put_item(data, 'signalplus_trade_queue')
                            # playground
                            if float(data["size"]) >= 10000:
                                redis_client.put_item(data, 'playground_trade_queue')
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
                strikes = []
                strikes_seen = {}
                expiries = []
                expiries_seen = {}
                prices = []
                premium = 0
                delta = 0
                gamma = 0
                vega = 0
                theta = 0
                rho = 0
                index_price = trades[0]["index_price"]
                total_size = 0
                currency = trades[0]["currency"]

                # sort trades by distances between trade["strike"] and index_price if trade["iv"] is not None
                trades = sorted(trades, key=lambda x: abs(float(x["symbol"].split("-")[-2]) - float(index_price)) if x["iv"] is not None else 0)
                # trade["symbol"]可能是"BTC-28JUN21-40000-C", "BTC-28JUN21-40000-P", "ETH-28JUN21-4000-C", "ETH-28JUN21-4000-P", "ETH-PERPETUAL", "ETH-14APR23"等格式。分解trades数据，得到callOrPut, strike, expiry并重新存入trades数组中
                for trade in trades:
                    if trade["symbol"].split("-")[-1] == "C" or trade["symbol"].split("-")[-1] == "P":
                        trade["callOrPut"] = trade["symbol"].split("-")[-1]
                        trade["strike"] = int(trade["symbol"].split("-")[-2])
                        trade["expiry"] = trade["symbol"].split("-")[-3]
                        if trade["strike"] not in strikes_seen:
                            strikes.append(trade["strike"])
                            strikes_seen[trade["strike"]] = True
                        if trade["expiry"] not in expiries_seen:
                            expiries.append(trade["expiry"])
                            expiries_seen[trade["expiry"]] = True
                        prices.append(f'{trade["price"]} ({str(trade["iv"])+"v"})')
                        direction = trade["direction"].upper()
                        if direction == "BUY":
                            size = float(trade["size"])
                        else:
                            size = -float(trade["size"])
                        premium += float(trade["price"]) * size
                        total_size += abs(size)
                        # if greeks
                        if "greeks" in trade:
                            delta += size * float(trade["greeks"]["delta"])
                            gamma += size * float(trade["greeks"]["gamma"])
                            vega += size * float(trade["greeks"]["vega"])
                            theta += size * float(trade["greeks"]["theta"])
                            rho += size * float(trade["greeks"]["rho"])
                    else:
                        trade["callOrPut"] = None
                        trade["strike"] = None
                        trade["expiry"] = None

                premium = premium / float(trades[0]["size"])

                result, size_ratio, legs = get_block_trade_strategy(trades)
                # 输出结果
                if result.empty or result["Strategy Name"].values[0] == "FUTURES SPREAD":
                    if result.empty:
                        strategy_name = "CUSTOM STRATEGY"
                        text = f"<b>CUSTOM {currency} STRATEGY:</b>"
                    else:
                        strategy_name = "FUTURES SPREAD"
                        text = f"<b>{currency} {strategy_name}:</b>"
                    text += '\n\n'

                    for trade in trades:
                        direction = trade["direction"].upper()
                        callOrPut = trade["symbol"].split("-")[-1]
                        if callOrPut == "C" or callOrPut == "P":
                            text += f'{"🔴 Sold" if direction=="SELL" else "🟢 Bought"} {trade["size"]}x '
                            text += f'{"🔶" if currency=="BTC" else "🔷"} {trade["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
                            text += f'at {trade["price"]} {"₿" if currency=="BTC" else "Ξ"} (${float(trade["price"])*float(trade["index_price"]):,.2f}) '
                            text += f'{"Total Sold:" if direction=="SELL" else "Total Bought:"} '
                            total_trade = float(trade["price"]) * float(trade["size"])
                            text += f'{total_trade:,.4f} {"₿" if currency=="BTC" else "Ξ"} (${total_trade*float(trade["index_price"])/1000:,.2f}K),'
                            text += f' <b>IV</b>: {str(trade["iv"])+"%"},'
                            text += f' <b>Ref</b>: {"$"+str(trade["index_price"])}'
                            text += f' {"‼️‼️" if (trade["currency"] == "BTC" and float(trade["size"]) >= 1000) or (trade["currency"] == "ETH" and float(trade["size"]) >= 10000) else ""}'
                            if "mark" in trade:
                                text += '\n'
                                text += f'bid: {trade["bid"]} (size: {trade["bid_amount"]}), mark: {trade["mark"]}, ask: {trade["ask"]} (size: {trade["ask_amount"]})'
                        else:
                            text += f'{"🔴 Sold " if direction=="SELL" else "🟢 Bought "} {trade["size"]}x '
                            text += f'{"🔶" if currency=="BTC" else "🔷"} {trade["symbol"]} '
                            text += f'at ${float(trade["price"]):,.2f}, '
                            text += f'<b>Ref</b>: {"$"+str(trade["index_price"])}'

                        text += '\n'

                else:
                    view = result["View"].values[0]
                    strategy_name = result["Strategy Name"].values[0]
                    short_strategy_name = result["Short Strategy Name"].values[0].title()
                    # strategy_name = 'LONG CALL SPREAD' or 'SHORT CALL SPREAD', make strategy_name to be 'LONG {currency} CALL SPREAD' or 'SHORT {currency} CALL SPREAD'
                    if strategy_name.startswith("LONG"):
                        strategy_name = strategy_name.replace("LONG", f"LONG {trades[0]['currency']}")
                        if size_ratio == "1:N" or size_ratio == "N:1":
                            trades = sorted(trades, key=lambda x: abs(float(x["symbol"].split("-")[-2]) - float(index_price)))
                            trade_summary = f'🟩 Bought {trades[0]["size"]}x/{trades[1]["size"]}x {"🔶" if currency=="BTC" else "🔷"} {trades[0]["currency"]} '
                        else:
                            trade_summary = f'🟩 Bought {trades[0]["size"]}x {"🔶" if currency=="BTC" else "🔷"} {trades[0]["currency"]} '
                    elif strategy_name.startswith("SHORT"):
                        strategy_name = strategy_name.replace("SHORT", f"SHORT {trades[0]['currency']}")
                        if size_ratio == "1:N" or size_ratio == "N:1":
                            trades = sorted(trades, key=lambda x: abs(float(x["symbol"].split("-")[-2]) - float(index_price)))
                            trade_summary = f'🟥 Sold {trades[0]["size"]}x/{trades[1]["size"]}x {"🔶" if currency=="BTC" else "🔷"} {trades[0]["currency"]} '
                        else:
                            trade_summary = f'🟥 Sold {trades[0]["size"]}x {"🔶" if currency=="BTC" else "🔷"} {trades[0]["currency"]} '
                        premium = -premium

                    if not pd.isna(view):
                        if size_ratio == "1:N" or size_ratio == "N:1":
                            text = f'<b>{strategy_name} ({view}) ({trades[0]["size"]}x/{trades[1]["size"]}x):</b>'
                        else:
                            text = f'<b>{strategy_name} ({view}) ({trades[0]["size"]}x):</b>'
                    else:
                        if size_ratio == "1:N" or size_ratio == "N:1":
                            text = f'<b>{strategy_name} ({trades[0]["size"]}x/{trades[1]["size"]}x):</b>'
                        else:
                            if legs == 1 and trades[0]["oi_change"] != 0:
                                if trades[0]["oi_change"] > 0:
                                    strategy_name = f'✅OPENED {strategy_name}'
                                else:
                                    if strategy_name.startswith("LONG"):
                                        strategy_name = strategy_name.replace("LONG", "SHORT")
                                    elif strategy_name.startswith("SHORT"):
                                        strategy_name = strategy_name.replace("SHORT", "LONG")
                                    strategy_name = f'❌CLOSED {strategy_name}'

                            text = f'<b>{strategy_name} ({trades[0]["size"]}x):</b>'
                    text += '\n'
                    if legs == 1:
                        data = trades[0]
                        direction = data["direction"].upper()
                        callOrPut = data["symbol"].split("-")[-1]

                        text += f'{"🔴 Sold" if direction=="SELL" else "🟢 Bought"} {data["size"]}x '
                        text += f'{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
                        text += f'at {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) '
                        text += f'{"Total Sold:" if direction=="SELL" else "Total Bought:"} '
                        total_trade = float(data["price"]) * float(data["size"])
                        text += f'{total_trade:,.4f} {"₿" if currency=="BTC" else "Ξ"} (${total_trade*float(data["index_price"])/1000:,.2f}K),'
                        text += f' <b>IV</b>: {str(data["iv"])+"%"},'
                        text += f' <b>Ref</b>: {"$"+str(data["index_price"])}'
                        text += f' {"‼️‼️" if (data["currency"] == "BTC" and float(data["size"]) >= 1000) or (data["currency"] == "ETH" and float(data["size"]) >= 10000) else ""}'
                        if "mark" in data:
                            text += '\n'
                            text += f'bid: {data["bid"]} (size: {data["bid_amount"]}), mark: {data["mark"]}, ask: {data["ask"]} (size: {data["ask_amount"]})'
                    else:
                        text += f'{trade_summary}'
                        if short_strategy_name.find("Calendar") != -1:
                            trades = sorted(trades, key=lambda x: datetime.strptime(x["expiry"], '%d%b%y'))
                            expiries = [trade["expiry"] for trade in trades]
                            prices = [f'{trade["price"]} ({str(trade["iv"])+"v"})' for trade in trades]
                        text += f'{"/".join(expiries)} '
                        text += f'{"/".join(map(str, strikes))} '
                        text += f'{short_strategy_name} '
                        text += f'at {premium:,.5f} {"₿" if currency=="BTC" else "Ξ"} (${premium*float(index_price):,.2f}) '
                        text += f' {"‼️‼️" if (trades[0]["currency"] == "BTC" and float(trades[0]["size"]) >= 1000) or (trades[0]["currency"] == "ETH" and float(trades[0]["size"]) >= 10000) else ""}'
                        text += '\n\n'
                        for trade in trades:
                            direction = trade["direction"].upper()
                            callOrPut = trade["symbol"].split("-")[-1]
                            if callOrPut == "C" or callOrPut == "P":
                                text += f'{"🔴 Sold" if direction=="SELL" else "🟢 Bought"} {trade["size"]}x '
                                text += f'{"🔶" if currency=="BTC" else "🔷"} {trade["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
                                text += f'at {trade["price"]} {"₿" if currency=="BTC" else "Ξ"} (${float(trade["price"])*float(trade["index_price"]):,.2f}) '
                                text += f'{"Total Sold:" if direction=="SELL" else "Total Bought:"} '
                                total_trade = float(trade["price"]) * float(trade["size"])
                                text += f'{total_trade:,.4f} {"₿" if currency=="BTC" else "Ξ"} (${total_trade*float(trade["index_price"])/1000:,.2f}K),'
                                text += f' <b>IV</b>: {str(trade["iv"])+"%"},'
                                text += f' <b>Ref</b>: {"$"+str(trade["index_price"])}'
                                if "mark" in trade:
                                    text += '\n'
                                    text += f'bid: {trade["bid"]} (size: {trade["bid_amount"]}), mark: {trade["mark"]}, ask: {trade["ask"]} (size: {trade["ask_amount"]})'
                                text += '\n'
                        # text += f'📊 <b>Leg Prices</b>: {", ".join(prices)}'
                        # text += f' <b>Ref</b>: {"$"+str(index_price)}'

                if delta != 0 or gamma != 0 or vega != 0 or theta != 0 or rho != 0:
                    text += '\n'
                    text += f'📖 <b>Risks</b>: <i>Δ: {delta:,.2f}, Γ: {gamma:,.4f}, ν: {vega:,.2f}, Θ: {theta:,.2f}, ρ: {rho:,.2f}</i>'
                text += '\n\n'
                text += f'<i>Deribit</i>'
                text += '\n'
                text += f'<i>#block</i>'
                # if timestamp in seconds of now % 3 is zero, add the text below
                # if int(time.time()) % 3 == 0:
                #     text += '\n'
                #     text += f'👉 Want Best Execution? <a href="https://pdgm.co/3ABtI6m">Paradigm</a> is 100% FREE and offers block liquidity in SIZE!'
                # TODO paradigm
                # if redis_client.is_paradigm_trade_timestamp_member(trades[0]["timestamp"]):
                #     text += f'<i> 👉 Block trades on <a href="https://www.paradigm.co">paradigm</a></i>'

                # push trade to Telegram
                try:
                    await bot.send_message(
                        chat_id=config.group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                except Exception as e:
                    logger.error(f"Failed to send message to main group {config.group_chat_id}: {e}")
                
                # push trade to SignalPlus (only once for all groups)
                await push_trade_to_signalplus(f"{currency} {strategy_name}", trades)

                # midas only
                if ((currency == "BTC" and float(total_size) >= 500) or (currency == "ETH" and float(total_size) >= 1000)):
                    try:
                        await bot.send_message(
                            chat_id=config.midas_group_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send message to midas group {config.midas_group_chat_id}: {e}")
                # signalplus only
                if ((currency == "BTC" and float(total_size) >= 500) or (currency == "ETH" and float(total_size) >= 5000)):
                    for chat_id in config.signalplus_group_chat_ids:
                        try:
                            await bot.send_message(
                                chat_id=chat_id,
                                text=text,
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                            )
                        except Exception as e:
                            logger.error(f"Failed to send message to signalplus group {chat_id}: {e}")
                # playground only
                if ((currency == "BTC" and float(total_size) >= 1000) or (currency == "ETH" and float(total_size) >= 10000)):
                    try:
                        await bot.send_message(
                            chat_id=config.playground_group_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send message to playground group {config.playground_group_chat_id}: {e}")
                # breavan horward only
                if ((currency == "BTC" and float(total_size) >= 49) or (currency == "ETH" and float(total_size) >= 999)):
                    try:
                        await bot.send_message(
                            chat_id=config.breavan_horward_group_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send message to breavan group {config.breavan_horward_group_chat_id}: {e}")
                # fbg only
                if ((currency == "BTC" and float(total_size) >= 100) or (currency == "ETH" and float(total_size) >= 1000)):
                    try:
                        await bot.send_message(
                            chat_id=config.fbg_group_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send message to fbg group {config.fbg_group_chat_id}: {e}")
                    for chat_id in config.default_blocktrade_group_chat_ids:
                        try:
                            await bot.send_message(
                                chat_id=chat_id,
                                text=text,
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                            )
                        except Exception as e:
                            logger.error(f"Failed to send message to default blocktrade group {chat_id}: {e}")
                # galaxy only
                if ((currency == "BTC" and float(total_size) >= 25) or (currency == "ETH" and float(total_size) >= 250)):
                    try:
                        await bot.send_message(
                            chat_id=config.galaxy_group_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send message to galaxy group {config.galaxy_group_chat_id}: {e}")
                # astron only
                if ((currency == "BTC" and float(total_size) >= 500) or (currency == "ETH" and float(total_size) >= 5000)):
                    try:
                        await bot.send_message(
                            chat_id=config.astron_group_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send message to astron group {config.astron_group_chat_id}: {e}")


                # # If id is like "midas_", then send the data to midas telegram group
                # if id.decode('utf-8').startswith("midas_"):
                #     await bot.send_message(
                #         chat_id=config.midas_group_chat_id,
                #         text=text,
                #         parse_mode=ParseMode.HTML,
                #         disable_web_page_preview=True,
                #     )
                # elif id.decode('utf-8').startswith("signalplus_"):
                #     for chat_id in config.signalplus_group_chat_ids:
                #         await bot.send_message(
                #             chat_id=chat_id,
                #             text=text,
                #             parse_mode=ParseMode.HTML,
                #             disable_web_page_preview=True,
                #         )
                # elif id.decode('utf-8').startswith("playground_"):
                #     await bot.send_message(
                #         chat_id=config.playground_group_chat_id,
                #         text=text,
                #         parse_mode=ParseMode.HTML,
                #         disable_web_page_preview=True,
                #     )
                # elif id.decode('utf-8').startswith("breavan_"):
                #     await bot.send_message(
                #         chat_id=config.breavan_horward_group_chat_id,
                #         text=text,
                #         parse_mode=ParseMode.HTML,
                #         disable_web_page_preview=True,
                #     )
                # elif id.decode('utf-8').startswith("galaxy_"):
                #     await bot.send_message(
                #         chat_id=config.galaxy_group_chat_id,
                #         text=text,
                #         parse_mode=ParseMode.HTML,
                #         disable_web_page_preview=True,
                #     )
                # elif id.decode('utf-8').startswith("astron_"):
                #     await bot.send_message(
                #         chat_id=config.astron_group_chat_id,
                #         text=text,
                #         parse_mode=ParseMode.HTML,
                #         disable_web_page_preview=True,
                #     )
                # elif id.decode('utf-8').startswith("fbg_"):
                #     try:
                #         await bot.send_message(
                #             chat_id=config.fbg_group_chat_id,
                #             text=text,
                #             parse_mode=ParseMode.HTML,
                #             disable_web_page_preview=True,
                #         )
                #     except Exception as e:
                #         print(e)
                #         print('unavailable', config.fbg_group_chat_id)
                #     for chat_id in config.default_blocktrade_group_chat_ids:
                #         try:
                #             await bot.send_message(
                #                 chat_id=chat_id,
                #                 text=text,
                #                 parse_mode=ParseMode.HTML,
                #                 disable_web_page_preview=True,
                #             )
                #         except Exception as e:
                #             print(e)
                #             print('unavailable', chat_id)
                # else:
                #     # push trade to SignalPlus
                #     await push_trade_to_signalplus(f"{currency} {strategy_name}", trades)
                #     # push trade to Telegram
                #     await bot.send_message(
                #         chat_id=config.group_chat_id,
                #         text=text,
                #         parse_mode=ParseMode.HTML,
                #         disable_web_page_preview=True,
                #     )
        except Exception as e:
            logger.error(f"Error5: {e}")
            continue
        # Wait for 10 second before fetching data again
        await asyncio.sleep(5)


async def push_advertisement_to_groups():
    while True:
        try:
            text = f'<b>🚀 <a href="https://pdgm.co/3ABtI6m">Paradigm</a>: Block size liquidity, tightest price. No fees</b>'
            for chat_id in config.all_group_chat_ids:
                await bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
        except Exception as e:
            logger.error(f"Advertisement Push Error: {e}")
            continue
        await asyncio.sleep(1800)


def get_block_trade_strategy(trades):
    # sort trades by strike if strike is not None, else by callOrPut and its value P<C if callOrPut is not none, else by expiry
    trades = sorted(trades, key=lambda x: (x["strike"] is None, x["strike"], x["callOrPut"] is None, x["callOrPut"] == "C", x["callOrPut"] == "P", x["expiry"] is None, x["expiry"]))

    # analyse trades to get legs, contract_type, strike, expiry, size_ratio, side
    legs = len(trades)
    contract_type = "N"
    strike = "N"
    expiry = "N"
    size_ratio = "N"
    side = "N"
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
        elif datetime.strptime(trades[0]["expiry"], '%d%b%y') < datetime.strptime(trades[1]["expiry"], '%d%b%y'):
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
        elif datetime.strptime(trades[0]["expiry"], '%d%b%y') < datetime.strptime(trades[1]["expiry"], '%d%b%y') < datetime.strptime(trades[2]["expiry"], '%d%b%y'):
            expiry = "A<B<C"
        elif datetime.strptime(trades[0]["expiry"], '%d%b%y') == datetime.strptime(trades[1]["expiry"], '%d%b%y') < datetime.strptime(trades[2]["expiry"], '%d%b%y'):
            expiry = "A=B<C"
        elif datetime.strptime(trades[0]["expiry"], '%d%b%y') < datetime.strptime(trades[1]["expiry"], '%d%b%y') == datetime.strptime(trades[2]["expiry"], '%d%b%y'):
            expiry = "A<B=C"
        elif datetime.strptime(trades[0]["expiry"], '%d%b%y') > datetime.strptime(trades[1]["expiry"], '%d%b%y') > datetime.strptime(trades[2]["expiry"], '%d%b%y'):
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

    return result, size_ratio, legs


# Define a function to send the data with prettify format to Telegram group
async def push_trade_to_telegram(group_chat_id):
    while True:
        try:
            # Pop data from Redis
            if group_chat_id == config.group_chat_id:
                data = redis_client.get_item('bigsize_trade_queue')
                if data:
                    text, strategy_name = generate_trade_message(data)
                    # push trade to SignalPlus
                    await push_trade_to_signalplus(f'{data["currency"]} {strategy_name}', [data])

                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
            elif group_chat_id == config.breavan_horward_group_chat_id:
                data = redis_client.get_item('breavan_trade_queue')
                if data:
                    text, _ = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
            elif group_chat_id == config.midas_group_chat_id:
                data = redis_client.get_item('midas_trade_queue')
                if data:
                    text, _ = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
            elif group_chat_id in config.signalplus_group_chat_ids:
                data = redis_client.get_item('signalplus_trade_queue')
                if data:
                    text, _ = generate_trade_message(data)
                    for chat_id in config.signalplus_group_chat_ids:
                        # Send the data to Telegram group
                        await bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
            elif group_chat_id == config.playground_group_chat_id:
                data = redis_client.get_item('playground_trade_queue')
                if data:
                    text, _ = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
            elif group_chat_id == config.galaxy_group_chat_id:
                data = redis_client.get_item('galaxy_trade_queue')
                if data:
                    text, _ = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
            elif group_chat_id == config.astron_group_chat_id:
                data = redis_client.get_item('astron_trade_queue')
                if data:
                    text, _ = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
            elif group_chat_id == config.fbg_group_chat_id:
                data = redis_client.get_item('fbg_trade_queue')
                if data:
                    text, _ = generate_trade_message(data)
                    # Send the data to Telegram group
                    try:
                        await bot.send_message(
                            chat_id=group_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send message to fbg group {group_chat_id}: {e}")
                    for chat_id in config.default_blocktrade_group_chat_ids:
                        # Send the data to Telegram group
                        try:
                            await bot.send_message(
                                chat_id=chat_id,
                                text=text,
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                            )
                        except Exception as e:
                            logger.error(f"Failed to send message to default group {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Error6: {e}")
            continue
        # Wait for 5 second before fetching data again
        await asyncio.sleep(5)


# generate a message with trade data
def generate_trade_message(data):
    direction = data["direction"].upper()
    callOrPut = data["symbol"].split("-")[-1]
    currency = data["currency"]
    # 根据direction和callOrPut判断strategy是"LONG CALL","SHORT CALL","LONG PUT"还是"SHORT PUT"
    if direction == "BUY":
        size = float(data["size"])
        if callOrPut == "C":
            strategy_name = f"LONG {currency} CALL"
            strategy = f"<b>LONG {currency} CALL ({size}x):</b>"
        elif callOrPut == "P":
            strategy_name = f"LONG {currency} PUT"
            strategy = f"<b>LONG {currency} PUT ({size}x):</b>"
    elif direction == "SELL":
        size = -float(data["size"])
        if callOrPut == "C":
            strategy_name = f"SHORT {currency} CALL"
            strategy = f"<b>SHORT {currency} CALL ({data['size']}x):</b>"
        elif callOrPut == "P":
            strategy_name = f"SHORT {currency} PUT"
            strategy = f"<b>SHORT {currency} PUT ({data['size']}x):</b>"

    if data["oi_change"] > 0:
        strategy = f'✅<b>OPENED</b> {strategy}'
    elif data["oi_change"] < 0:
        if strategy.startswith("<b>LONG"):
            strategy = strategy.replace("LONG", "SHORT")
        elif strategy.startswith("<b>SHORT"):
            strategy = strategy.replace("SHORT", "LONG")
        strategy = f'❌<b>CLOSED</b> {strategy}'

    text = strategy
    text += '\n\n'
    # text += f'<b><i>📝 {data["source"].upper()} {data["trade_id"]}</i></b>'
    # text += '\n'
    # text += f'<i>🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC</i>'
    text += f'{"🔴 Sold" if direction=="SELL" else "🟢 Bought"} {data["size"]}x '
    text += f'{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
    text += f'at {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) '
    text += f'{"Total Sold:" if direction=="SELL" else "Total Bought:"} '
    total_trade = float(data["price"]) * float(data["size"])
    text += f'{total_trade:,.4f} {"₿" if currency=="BTC" else "Ξ"} (${total_trade*float(data["index_price"])/1000:,.2f}K),'
    text += f' <b>IV</b>: {str(data["iv"])+"%"},'
    text += f' <b>Ref</b>: {"$"+str(data["index_price"])}'
    text += f' {"‼️‼️" if (data["currency"] == "BTC" and float(data["size"]) >= 1000) or (data["currency"] == "ETH" and float(data["size"]) >= 10000) else ""}'
    if "mark" in data:
        text += '\n'
        text += f'bid: {data["bid"]} (size: {data["bid_amount"]}), mark: {data["mark"]}, ask: {data["ask"]} (size: {data["ask_amount"]})'

    # text += '\n\n'
    # text += f'📊{" <b>Vol</b>: "+str(data["iv"])+"%," if data["iv"] else ""}'
    # text += f' <b>Ref</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}'
    if "greeks" in data:
        text += '\n'
        delta = float(data["greeks"]["delta"]) * size
        gamma = float(data["greeks"]["gamma"]) * size
        vega = float(data["greeks"]["vega"]) * size
        theta = float(data["greeks"]["theta"]) * size
        rho = float(data["greeks"]["rho"]) * size
        text += f'📖 <b>Risks</b>: <i>Δ: {delta:,.2f}, Γ: {gamma:,.4f}, ν: {vega:,.2f}, Θ: {theta:,.2f}, ρ: {rho:,.2f}</i>'
    text += '\n\n'
    text += f'<i>{data["source"].title()}</i>'
    text += '\n'
    if "liquidation" in data and data["liquidation"]:
        text += f'<i>#liquidation</i>'
    else:
        text += f'<i>#onscreen</i>'
    # if timestamp in seconds of now % 3 is zero, add the text below
    # if int(time.time()) % 3 == 0:
    #     text += '\n'
    #     text += f'👉 Want Best Execution? <a href="https://pdgm.co/3ABtI6m">Paradigm</a> is 100% FREE and offers block liquidity in SIZE!'
    return text, strategy_name


# push data to signalplus server
async def push_trade_to_signalplus(strategy_name, trades):
    headers = {
        "Content-Type" : "application/json"
    }
    req_body = {
        "accessKey": config.signalplus_push_trade_key,
        "secretKey": config.signalplus_push_trade_secret,
        "strategy_name": strategy_name,
        "trades": trades
    }

    try:
        response = requests.post(SIGNALPLUS_PUSH_TRADE_API, headers=headers, json=req_body)
    except Exception as e:
        logger.error(e)
        return

    rsp_dict = response.json()
    code = rsp_dict.get("code", -1)
    if code != 0:
        logger.error(f"SignalPlus Error: failed to push blocktrade to SignalPlus , code = {code}")



def run_bot() -> None:
    # Create two threads to fetch block trade data and send it to Telegram group by using asyncio
    try:
        loop = asyncio.get_event_loop()
        # TODO paradigm trade timestamp
        # loop.create_task(fetch_paradigm_trade_timestamp())
        loop.create_task(fetch_deribit_data_all())
        loop.create_task(fetch_okx_data_all())
        loop.create_task(fetch_bybit_data_all())
        loop.create_task(handle_trade_data())
        loop.create_task(push_trade_to_telegram(config.group_chat_id))
        loop.create_task(push_trade_to_telegram(config.breavan_horward_group_chat_id))
        loop.create_task(push_trade_to_telegram(config.midas_group_chat_id))
        loop.create_task(push_trade_to_telegram(config.fbg_group_chat_id))
        loop.create_task(push_trade_to_telegram(config.galaxy_group_chat_id))
        loop.create_task(push_trade_to_telegram(config.astron_group_chat_id))
        loop.create_task(push_trade_to_telegram(config.signalplus_group_chat_ids[0]))
        loop.create_task(push_trade_to_telegram(config.playground_group_chat_id))
        loop.create_task(push_block_trade_to_telegram())
        # loop.create_task(push_advertisement_to_groups())
        loop.run_forever()
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    run_bot()
