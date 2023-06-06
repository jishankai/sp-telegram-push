#!/usr/bin/env python3

import time
import datetime
import requests
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import io
import sys
import telegram
from telegram.constants import ParseMode
import asyncio
import yaml
from pathlib import Path

config_dir = Path(__file__).parent.parent.resolve() / "config"
assets_dir = Path(__file__).parent.parent.resolve() / "assets"
# load yaml config
with open(config_dir / "config.yml", 'r') as f:
    config_yaml = yaml.safe_load(f)
bot = telegram.Bot(token=config_yaml["telegram_token"])
SIGNALPLUS_VOLUME_TRADE_API = "https://mizar-gateway.signalplus.com/mizar/block_trades/querySum"
plt.rcParams['font.family'] = 'monospace'


def push_volume():
    args = sys.argv[1:]
    currency = args[0].upper()
    exchange_name = args[1].lower()
    headers =   {
        "Content-Type" : "application/json"
    }
    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    req_body = {
        "accessKey": config_yaml["signalplus_push_trade_key"],
        "secretKey": config_yaml["signalplus_push_trade_secret"],
        "startTime": start_time,
        "endTime": end_time,
        "currency": currency,
        "source": exchange_name,
    }
    response = requests.post(SIGNALPLUS_VOLUME_TRADE_API, headers=headers, json=req_body)
    json_data = response.json()
    json_data['value'] = [item for item in json_data['value'] if item['symbol'] != f'{currency}-PERPETUAL']

    data = [['Rank', 'Instrument', 'Size']]
    for i, item in enumerate(json_data['value']):
        parts = item['symbol'].split('-')
        date = datetime.datetime.strptime(parts[1], "%d%b%y")
        new_date_str = date.strftime("%y%m%d")
        new_symbol = f"{parts[0]}-{new_date_str}-{parts[2]}-{parts[3]}"
        data.append([i+1, new_symbol, item['size']])
        if i == 9:
            break

    # 创建新的图例
    title_text = f'{currency} {exchange_name.upper()} 24H BLOCK TRADE VOLUME TOP 10'
    now = datetime.datetime.now()
    footer_text = now.strftime("%Y-%m-%d %H:%M:%S")
    fig_background_color = 'snow'
    fig_border = 'darkgray'
    column_headers = data.pop(0)
    #row_headers = [x.pop(0) for x in data]
    # Table data needs to be non-numeric text. Format the data
    # while I'm at it.
    cell_text = []
    for row in data:
        cell_text.append([x for x in row])
    # Get some lists of color specs for row and column headers
    #rcolors = plt.cm.BuPu(np.full(len(row_headers), 0.1))
    ccolors = plt.cm.BuPu(np.full(len(column_headers), 0.1))
    # Create the figure. Setting a small pad on tight_layout
    # seems to better regulate white space. Sometimes experimenting
    # with an explicit figsize here can produce better outcome.
    plt.figure(linewidth=2,
               edgecolor=fig_border,
               facecolor=fig_background_color,
               tight_layout={'pad':1},
               #figsize=(5,3)
               )
    # Add a table at the bottom of the axes
    the_table = plt.table(cellText=cell_text,
                          #rowLabels=row_headers,
                          #rowColours=rcolors,
                          cellLoc='center',
                          colWidths=[0.15, 1/3, 1/3],
                          colColours=ccolors,
                          colLabels=column_headers,
                          loc='center')
    # Scaling is the only influence we have over top and bottom cell padding.
    # Make the rows taller (i.e., make cell y scale larger).
    the_table.scale(1, 1.5)
    # Hide axes
    ax = plt.gca()
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    # Hide axes border
    plt.box(on=None)
    # Add title
    plt.suptitle(title_text, y=0.92)
    # Add footer
    plt.figtext(0.05, 0.05, footer_text, horizontalalignment='left', size=6, weight='light')
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


    plt.show()

    # buf = io.BytesIO()
    # plt.savefig(buf, format='png', dpi=fig.dpi)
    # buf.seek(0)
    # text = f'📊 {title_text}'
    # text += '\n\n'
    # if currency == 'BTC':
    #     text += '<b>🚀 <a href="https://pdgm.co/3ABtI6m">Paradigm</a>: Block size liquidity, tightest price. No fees</b>'
    # else:
    #     text += '<b>📈 <a href="https://t.signalplus.com/user/login?redirect=%2Fdashboard">SignalPlus</a>: Advanced options trading with zero fees</b>'
    # asyncio.run(bot.send_photo(chat_id=config_yaml["group_chat_id"], photo=buf, caption=text, parse_mode=ParseMode.HTML))


if __name__ == "__main__":
    push_volume()
