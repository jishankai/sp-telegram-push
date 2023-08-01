#!/usr/bin/env python3

import requests
import asyncio
import datetime
import time
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import io
import sys
import telegram
from telegram.constants import ParseMode
import flag
import yaml
from pathlib import Path

config_dir = Path(__file__).parent.parent.resolve() / "config"
assets_dir = Path(__file__).parent.parent.resolve() / "assets"
# load yaml config
with open(config_dir / "config.yml", 'r') as f:
    config_yaml = yaml.safe_load(f)
bot = telegram.Bot(token=config_yaml["telegram_token"])
plt.rcParams['font.family'] = 'monospace'


# 定义获取价格的函数
def get_crypto_prices():
    # 定义 CoinGecko API 的 URL
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin%2Cethereum&vs_currencies=usd'

    # 发送 GET 请求获取价格数据
    response = requests.get(url)
    # 解析响应数据
    prices = response.json()
    # 获取 BTC 和 ETH 的价格
    btc_price = prices['bitcoin']['usd']
    eth_price = prices['ethereum']['usd']

    return btc_price, eth_price


async def get_calendar():
    oAuthUrl = "https://authorization.fxstreet.com/v2/token"
    oAuthData = {
        "grant_type": "client_credentials",
        "client_id": config_yaml["fxstreet_public_key"],
        "client_secret": config_yaml["fxstreet_private_key"],
        "scope": "calendar"
    }
    oAuthResponse = requests.post(oAuthUrl, data=oAuthData)
    oAuth = oAuthResponse.json()

    calendarUrl = "https://calendar-api.fxstreet.com/en/api/v1/eventDates"
    headers = {
        "Authorization": f'{oAuth["token_type"]} {oAuth["access_token"]}'
    }
    # current date utc string
    currentDate = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    # tomorrow date utc string
    tomorrowDate = (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # after tomorrow date utc string
    afterTomorrowDate = (datetime.datetime.utcnow() + datetime.timedelta(days=2)).strftime("%Y-%m-%d")

    todayCalendarResponse = requests.get(f'{calendarUrl}/{currentDate}/{tomorrowDate}', params={ "volatilities": "HIGH" }, headers=headers)
    tomorrowCalendarResponse = requests.get(f'{calendarUrl}/{tomorrowDate}/{afterTomorrowDate}', params={ "volatilities": "HIGH" }, headers=headers)
    todayCalendar = todayCalendarResponse.json()
    tomorrowCalendar = tomorrowCalendarResponse.json()
    calendarFlitered = []
    if type(todayCalendar) is not dict:
        # calendar = [{'id': '6c272cfe-3881-4fb0-9ef8-e7362fb967fe', 'eventId': '28fc0440-6e9f-49b5-a847-d6d88fc6825c', 'dateUtc': '2023-07-28T01:30:00Z', 'periodDateUtc': '2023-04-01T00:00:00Z', 'periodType': 'QUARTER', 'actual': 0.5, 'revised': 0.7, 'consensus': 0.9, 'ratioDeviation': -0.48593, 'previous': 1.0, 'isBetterThanExpected': False, 'name': 'Producer Price Index (QoQ)', 'countryCode': 'AU', 'currencyCode': 'AUD', 'unit': '%', 'potency': 'ZERO', 'volatility': 'LOW', 'isAllDay': False, 'isTentative': False, 'isPreliminary': False, 'isReport': False, 'isSpeech': False, 'lastUpdated': 1690523985, 'previousIsPreliminary': None}, {'id': '9985c67e-4eb8-4df0-abdb-c48c1b0b3dc5', 'eventId': 'f6b00222-707d-4379-8965-b66ec535fac6', 'dateUtc': '2023-07-28T01:30:00Z', 'periodDateUtc': '2023-04-01T00:00:00Z', 'periodType': 'QUARTER', 'actual': 3.9, 'revised': 4.9, 'consensus': 3.9, 'ratioDeviation': 0.0, 'previous': 5.2, 'isBetterThanExpected': None, 'name': 'Producer Price Index (YoY)', 'countryCode': 'AU', 'currencyCode': 'AUD', 'unit': '%', 'potency': 'ZERO', 'volatility': 'MEDIUM', 'isAllDay': False, 'isTentative': False, 'isPreliminary': False, 'isReport': False, 'isSpeech': False, 'lastUpdated': 1690523946, 'previousIsPreliminary': None}]
        # filter dateUtc, name, countryCode, unit, potency, actual, consensus, previous from calendar
        for event in todayCalendar:
            dateUtc = datetime.datetime.strptime(event["dateUtc"], "%Y-%m-%dT%H:%M:%SZ")
            dateHour = dateUtc.strftime("%H")
            if dateHour < "06":
                continue
            calendarFlitered.append({
                "dateUtc": dateUtc,
                "name": event["name"],
                "countryCode": event["countryCode"],
                "unit": event["unit"],
                "potency": event["potency"],
                "actual": event["actual"],
                "consensus": event["consensus"],
                "previous": event["previous"]
            })

    if type(tomorrowCalendar) is not dict:
        for event in todayCalendar:
            dateUtc = datetime.datetime.strptime(event["dateUtc"], "%Y-%m-%dT%H:%M:%SZ")
            dateHour = dateUtc.strftime("%H")
            if dateHour >= "06":
                continue
            calendarFlitered.append({
                "dateUtc": dateUtc,
                "name": event["name"],
                "countryCode": event["countryCode"],
                "unit": event["unit"],
                "potency": event["potency"],
                "actual": event["actual"],
                "consensus": event["consensus"],
                "previous": event["previous"]
            })

    if len(calendarFlitered) == 0:
        return

    # data = [["Time", "Event", "Area", "Actual", "Consensus", "Previous"]]
    data = [["UTC+0", "Event", "Area", "Consensus"]]
    for event in calendarFlitered:
        # convert datetime to string
        dateLocalString = event["dateUtc"].strftime("%H:%M")
        # covert countryCode to flag
        flag_emoji = flag.flag(event["countryCode"])
        # convert actual, consensus, previous to string
        actualString = "-"
        consensusString = "-"
        previousString = "-"
        if event["actual"] is not None:
            if event["unit"] == "%":
                actualString = f'{event["actual"]}%'
            elif event["potency"] in ["K", "M", "B"]:
                actualString = f'{event["actual"]}{event["potency"]}'
            else:
                actualString = str(event["actual"])
        if event["consensus"] is not None:
            if event["unit"] == "%":
                consensusString = f'{event["consensus"]}%'
            elif event["potency"] in ["K", "M", "B"]:
                consensusString = f'{event["consensus"]}{event["potency"]}'
            else:
                consensusString = str(event["consensus"])
        if event["previous"] is not None:
            if event["unit"] == "%":
                previousString = f'{event["previous"]}%'
            elif event["potency"] in ["K", "M", "B"]:
                previousString = f'{event["previous"]}{event["potency"]}'
            else:
                previousString = str(event["previous"])
        #data.append([dateLocalString, event["name"], f'{event["countryCode"]}', actualString, consensusString, previousString])
        data.append([dateLocalString, event["name"], f'{event["countryCode"]}', consensusString])

    # get crypto prices
    btc_price, eth_price = get_crypto_prices()

    # create new figure
    title_text = f'Economic Calendar {currentDate} - {tomorrowDate}'
    now = datetime.datetime.now()
    footer_text = now.strftime('%Y-%m-%d %H:%M:%S')
    footer_text += " " + r"$\bf{BTC}$" + f":\${btc_price} " + r"$\bf{ETH}$" + f":\${eth_price}"
    fig_background_color = 'snow'
    fig_border = 'darkgray'
    column_headers = data.pop(0)
    cell_text = []
    for row in data:
        cell_text.append([x for x in row])
    ccolors = plt.cm.BuPu(np.full(len(column_headers), 0.1))
    plt.figure(linewidth=2,
               edgecolor=fig_border,
               facecolor=fig_background_color,
               tight_layout={'pad':1},
               figsize=(10, 10)
               )
    # Add a table at the bottom of the axes
    the_table = plt.table(cellText=cell_text,
                          #rowLabels=row_headers,
                          #rowColours=rcolors,
                          cellLoc='center',
                          colWidths=[1/10, 7/10, 1/10, 1/10],
                          colColours=ccolors,
                          colLabels=column_headers,
                          loc='center')
    set_align_for_column(the_table, col=0, align="center")
    set_align_for_column(the_table, col=1, align="left")
    set_align_for_column(the_table, col=2, align="center")
    the_table.auto_set_font_size(False)
    the_table.set_fontsize(12)
    # Scaling is the only influence we have over top and bottom cell padding.
    # Make the rows taller (i.e., make cell y scale larger).
    the_table.scale(1, 3)
    # Hide axes
    ax = plt.gca()
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    # Hide axes border
    plt.box(on=None)
    # Add title
    plt.suptitle(title_text, y=0.92, fontsize=16, weight='bold', color='black')
    # Add footer
    plt.figtext(0.02, 0.03, footer_text, horizontalalignment='left', size=13, weight='medium')
    # Force the figure to update, so backends center objects correctly within the figure.
    # Without plt.draw() here, the title will center on the axes and not the figure.
    plt.draw()
    # Create image. plt.savefig ignores figure edge and face colors, so map them.
    fig = plt.gcf()
    img = Image.open(f'{assets_dir}/logo.png')
    width, height = fig.get_size_inches()*fig.dpi
    wm_width = int(width/4)
    scaling = (wm_width / float(img.size[0]))
    wm_height = int(float(img.size[1])*float(scaling))
    img = img.resize((wm_width, wm_height), Image.LANCZOS)
    # ax = plt.axes()
    # xpos = ax.transAxes.transform((0.695,0))[0]
    # ypos = ax.transAxes.transform((0,0.805))[1]
    fig.text(0.5, 0.5, 'SignalPlus',
             fontsize=40, color='black',
             ha='center', va='center', alpha=0.1)
    fig.figimage(img, width-wm_width, 0, alpha=.8, zorder=1)

    buf = io.BytesIO()
    # plt.savefig('test.png', format='png', dpi=fig.dpi)
    plt.savefig(buf, format='png', dpi=fig.dpi)
    text = f'📅 {title_text}'
    text += '\n\n'
    # text += '<b>📈 <a href="https://t.signalplus.com/user/login?redirect=%2Fdashboard">SignalPlus</a>: Advanced options trading with zero fees</b>'

    # all groups
    for group_chat_id in config_yaml["all_group_chat_ids"]:
        try:
            buf.seek(0)
            await bot.send_photo(chat_id=group_chat_id, photo=buf, caption=text, parse_mode=ParseMode.HTML)
            print('sent', group_chat_id)
        except Exception as e:
            print(e)
            print('unavailable', group_chat_id)


def set_align_for_column(table, col, align="left"):
    cells = [key for key in table._cells if key[1] == col]
    for cell in cells:
        table._cells[cell]._loc = align
        table._cells[cell]._text.set_horizontalalignment(align)

if __name__ == "__main__":
    asyncio.run(get_calendar())
