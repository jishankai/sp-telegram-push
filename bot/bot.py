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
import paradigm

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DERIBIT_TRADE_API = "https://www.deribit.com/api/v2/public/get_last_trades_by_currency"
DERIBIT_TICKER_API = "https://www.deribit.com/api/v2/public/ticker"
BYBIT_TRADE_API = "https://api-testnet.bybit.com/v5/market/recent-trade"
BYBIT_SYMBOL_API = "https://api-testnet.bybit.com/v5/market/instruments-info"
OKX_TRADE_API = "https://www.okx.com/api/v5/public/option-trades"

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
                    trade = {
                        "trade_id": trade["trade_id"],
                        "source": "deribit",
                        "symbol": trade["instrument_name"],
                        "currency": currency,
                        "direction": trade["direction"],
                        "price": trade["price"],
                        "size": trade["amount"],
                        "iv": trade["iv"],
                        "greeks": greeks,
                        "index_price": trade["index_price"],
                        "liquidation": True if "liquidation" in trade else False,
                        "timestamp": trade["timestamp"],
                    }
                else:
                    trade = {
                        "trade_id": trade["trade_id"],
                        "source": "deribit",
                        "symbol": trade["instrument_name"],
                        "currency": currency,
                        "direction": trade["direction"],
                        "price": trade["price"],
                        "size": trade["amount"],
                        "iv": None,
                        "index_price": trade["index_price"],
                        "liquidation": True if "liquidation" in trade else False,
                        "timestamp": trade["timestamp"],
                    }
                if not redis_client.is_block_trade_id_member(block_trade_id):
                    redis_client.put_block_trade_id(block_trade_id)
                redis_client.put_block_trade(trade, block_trade_id)

                # midas only
                if ((trade["currency"] == "BTC" and float(trade["size"]) >= 500) or (trade["currency"] == "ETH" and float(trade["size"]) >= 1000)) and trade["iv"] is not None:
                    if not redis_client.is_block_trade_id_member(f"midas_{block_trade_id}"):
                        redis_client.put_block_trade_id(f"midas_{block_trade_id}")
                    redis_client.put_block_trade(trade, f"midas_{block_trade_id}")
                # signalplus only
                if ((trade["currency"] == "BTC" and float(trade["size"]) >= 500) or (trade["currency"] == "ETH" and float(trade["size"]) >= 2000)) and trade["iv"] is not None:
                    if not redis_client.is_block_trade_id_member(f"signalplus_{block_trade_id}"):
                        redis_client.put_block_trade_id(f"signalplus_{block_trade_id}")
                    redis_client.put_block_trade(trade, f"signalplus_{block_trade_id}")
                # playground only
                if ((trade["currency"] == "BTC" and float(trade["size"]) >= 1000) or (trade["currency"] == "ETH" and float(trade["size"]) >= 10000)) and trade["iv"] is not None:
                    if not redis_client.is_block_trade_id_member(f"playground_{block_trade_id}"):
                        redis_client.put_block_trade_id(f"playground_{block_trade_id}")
                    redis_client.put_block_trade(trade, f"playground_{block_trade_id}")

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
                # get greeks if big size
                if (trade["currency"] == "BTC" and float(trade["size"]) >= 25) or (trade["currency"] == "ETH" and float(trade["size"]) >= 250):
                    ticker = requests.get(DERIBIT_TICKER_API, params={
                        "instrument_name": trade["symbol"],
                    }).json()
                    trade["greeks"] = ticker["result"]["greeks"]

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
                logger.error(f"Pop data from Redis: {data}")
                # Check if the size is >=25 or >=250
                if data["currency"] == "BTC" and float(data["size"]) >= 25:
                    redis_client.put_item(data, 'bigsize_trade_queue')
                    # midas only
                    if float(data["size"]) >= 500:
                        redis_client.put_item(data, 'midas_trade_queue')
                        # signalplus
                        redis_client.put_item(data, 'signalplus_trade_queue')
                        # playground
                        if float(data["size"]) >= 1000:
                            redis_client.put_item(data, 'playground_trade_queue')
                elif data["currency"] == "ETH" and float(data["size"]) >= 250:
                    redis_client.put_item(data, 'bigsize_trade_queue')
                    # midas only
                    if float(data["size"]) >= 1000:
                        redis_client.put_item(data, 'midas_trade_queue')
                        # signalplus
                        if float(data["size"]) >= 2000:
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
                expiries = []
                prices = []
                premium = 0
                delta = 0
                gamma = 0
                vega = 0
                theta = 0
                rho = 0

                # trade["symbol"]可能是"BTC-28JUN21-40000-C", "BTC-28JUN21-40000-P", "ETH-28JUN21-4000-C", "ETH-28JUN21-4000-P", "ETH-PERPETUAL", "ETH-14APR23"等格式。分解trades数据，得到callOrPut, strike, expiry并重新存入trades数组中
                for trade in trades:
                    if trade["symbol"].split("-")[-1] == "C" or trade["symbol"].split("-")[-1] == "P":
                        trade["callOrPut"] = trade["symbol"].split("-")[-1]
                        trade["strike"] = trade["symbol"].split("-")[-2]
                        trade["expiry"] = trade["symbol"].split("-")[-3]
                        strikes.append(trade["strike"])
                        expiries.append(trade["expiry"])
                        prices.append(f'{trade["price"]}({str(trade["iv"])+"v"})')
                        direction = trade["direction"].upper()
                        if direction == "BUY":
                            size = float(trade["size"])
                        else:
                            size = -float(trade["size"])
                        premium += float(trade["price"]) * float(size)
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

                expiries = set(expiries)
                strikes = set(strikes)
                premium = premium / float(trades[0]["size"])

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
                    text = f"<b>CUSTOM {trades[0]['currency']} STRATEGY:</b>"
                    for trade in trades:
                        direction = trade["direction"].upper()
                        callOrPut = trade["symbol"].split("-")[-1]
                        currency = trade["currency"]
                        if callOrPut == "C" or callOrPut == "P":
                            text += '\n\n'
                            text += f'{"🔴 Sold" if direction=="SELL" else "🟢 Bought"} {trade["size"]}x '
                            text += f'{"🔶" if currency=="BTC" else "🔷"} {trade["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
                            text += f'at {trade["price"]} {"U" if trade["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${trade["price"] if trade["source"].upper()=="BYBIT" else float(trade["price"])*float(trade["index_price"]):,.2f}) '
                            text += f' {"‼️‼️" if (trade["currency"] == "BTC" and float(trade["size"]) >= 1000) or (trade["currency"] == "ETH" and float(trade["size"]) >= 10000) else ""}'
                            text += '\n'
                            text += f'📊 <b>Vol</b>: {str(trade["iv"])+"%"},'
                            text += f' <b>Ref</b>: {"$"+str(trade["index_price"])}'
                        else:
                            text += '\n'
                            text += f'{"🔴 Sold " if direction=="SELL" else "🟢 Bought "} {trade["size"]}x '
                            text += f'{"🔶" if currency=="BTC" else "🔷"} {trade["symbol"]} '
                            text += f'at ${float(trade["price"]):,.2f} '
                            text += '\n\n'
                            text += f'📊 <b>Ref</b>: {"$"+str(trade["index_price"])}'

                else:
                    view = result["View"].values[0]
                    strategy_name = result["Strategy Name"].values[0]
                    short_strategy_name = result["Short Strategy Name"].values[0].title()
                    # strategy_name = 'LONG CALL SPREAD' or 'SHORT CALL SPREAD', make strategy_name to be 'LONG {currency} CALL SPREAD' or 'SHORT {currency} CALL SPREAD'
                    if strategy_name.startswith("LONG"):
                        strategy_name = strategy_name.replace("LONG", f"LONG {trades[0]['currency']}")
                        trade_summary = f'🟩 Bought {trades[0]["size"]}x {trades[0]["currency"]} '
                    elif strategy_name.startswith("SHORT"):
                        strategy_name = strategy_name.replace("SHORT", f"SHORT {trades[0]['currency']}")
                        trade_summary = f'🟥 Sold {trades[0]["size"]}x {trades[0]["currency"]} '
                        premium = -premium

                    if not pd.isna(view):
                        text = f'<b>{strategy_name} ({view}) ({trades[0]["size"]}x):</b>'
                    else:
                        text = f'<b>{strategy_name} ({trades[0]["size"]}x):</b>'
                    text += '\n\n'
                    text += f'{trade_summary}'
                    text += f'{"/".join(expiries)} '
                    text += f'{"/".join(strikes)} '
                    text += f'{short_strategy_name} '
                    text += f'at {premium} '
                    text += f' {"‼️‼️" if (trades[0]["currency"] == "BTC" and float(trades[0]["size"]) >= 1000) or (trades[0]["currency"] == "ETH" and float(trades[0]["size"]) >= 10000) else ""}'
                    text += '\n\n'
                    text += f'📊 <b>Leg Prices</b>: {", ".join(prices)}'
                    text += f' <b>Ref</b>: {"$"+str(trades[0]["index_price"])}'

                if delta != 0 or gamma != 0 or vega != 0 or theta != 0 or rho != 0:
                    text += '\n'
                    text += f'📖 <b>Greeks</b>: <i>Δ: {delta:,.5f}, Γ: {gamma:,.5f}, ν: {vega:,.5f}, Θ: {theta:,.5f}, ρ: {rho:,.5f}</i>'
                text += '\n\n'
                text += f'<i>Deribit</i>'
                text += '\n'
                text += f'<i>#block</i>'
                # TODO paradigm
                # if redis_client.is_paradigm_trade_timestamp_member(trades[0]["timestamp"]):
                #     text += f'<i> 👉 Block trades on <a href="https://www.paradigm.co">paradigm</a></i>'

                # If id is like "midas_", then send the data to midas telegram group
                if id.decode('utf-8').startswith("midas_"):
                    await bot.send_message(
                        chat_id=config.midas_group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                    )
                elif id.decode('utf-8').startswith("signalplus_"):
                    for chat_id in config.signalplus_group_chat_ids:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                        )
                elif id.decode('utf-8').startswith("playground_"):
                    await bot.send_message(
                        chat_id=config.playground_group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                    )
                else:
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
async def push_trade_to_telegram(group_chat_id):
    while True:
        try:
            # Pop data from Redis
            if group_chat_id == config.group_chat_id:
                data = redis_client.get_item('bigsize_trade_queue')
                if data:
                    text = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                    )
            elif group_chat_id == config.midas_group_chat_id:
                data = redis_client.get_item('midas_trade_queue')
                if data:
                    text = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                    )
            elif group_chat_id in config.signalplus_group_chat_ids:
                data = redis_client.get_item('signalplus_trade_queue')
                if data:
                    text = generate_trade_message(data)
                    for chat_id in config.signalplus_group_chat_ids:
                        # Send the data to Telegram group
                        await bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                        )
            elif group_chat_id == config.playground_group_chat_id:
                data = redis_client.get_item('playground_trade_queue')
                if data:
                    text = generate_trade_message(data)
                    # Send the data to Telegram group
                    await bot.send_message(
                        chat_id=group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                    )
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
            strategy = f"<b>LONG {currency} CALL ({size}x):</b>"
        elif callOrPut == "P":
            strategy = f"<b>LONG {currency} PUT ({size}x):</b>"
    elif direction == "SELL":
        size = -float(data["size"])
        if callOrPut == "C":
            strategy = f"<b>SHORT {currency} CALL ({data['size']}x):</b>"
        elif callOrPut == "P":
            strategy = f"<b>SHORT {currency} PUT ({data['size']}x):</b>"

    text = strategy
    text += '\n'
    # text += f'<b><i>📝 {data["source"].upper()} {data["trade_id"]}</i></b>'
    # text += '\n'
    # text += f'<i>🕛 {datetime.fromtimestamp(int(data["timestamp"])//1000)} UTC</i>'
    text += f'{"🔴 Sold" if direction=="SELL" else "🟢 Bought"} {data["size"]}x '
    text += f'{"🔶" if currency=="BTC" else "🔷"} {data["symbol"]} {"📈" if callOrPut=="C" else "📉"} '
    text += f'at {data["price"]} {"U" if data["source"].upper()=="BYBIT" else "₿" if currency=="BTC" else "Ξ"} (${data["price"] if data["source"].upper()=="BYBIT" else float(data["price"])*float(data["index_price"]):,.2f}) '
    text += f' {"‼️‼️" if (data["currency"] == "BTC" and float(data["size"]) >= 1000) or (data["currency"] == "ETH" and float(data["size"]) >= 10000) else ""}'
    text += '\n\n'
    text += f'📊{" <b>Vol</b>: "+str(data["iv"])+"%," if data["iv"] else ""}'
    text += f' <b>Ref</b>: {"$"+str(data["index_price"]) if data["index_price"] else "Unknown"}'
    if "greeks" in data:
        text += '\n'
        delta = float(data["greeks"]["delta"]) * size
        gamma = float(data["greeks"]["gamma"]) * size
        vega = float(data["greeks"]["vega"]) * size
        theta = float(data["greeks"]["theta"]) * size
        rho = float(data["greeks"]["rho"]) * size
        text += f'📖 <b>Greeks</b>: <i>Δ: {delta:,.5f}, Γ: {gamma:,.5f}, ν: {vega:,.5f}, Θ: {theta:,.5f}, ρ: {rho:,.5f}</i>'
    text += '\n\n'
    text += f'<i>{data["source"].title()}</i>'
    text += '\n'
    if "liquidation" in data and data["liquidation"]:
        text += f'<i>#liquidation</i>'
    else:
        text += f'<i>#onscreen</i>'
    return text

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
        loop.create_task(push_trade_to_telegram(config.midas_group_chat_id))
        loop.create_task(push_trade_to_telegram(config.signalplus_group_chat_ids[0]))
        loop.create_task(push_trade_to_telegram(config.playground_group_chat_id))
        loop.create_task(push_block_trade_to_telegram())
        loop.run_forever()
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    run_bot()
