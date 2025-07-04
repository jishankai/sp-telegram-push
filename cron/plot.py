#!/usr/bin/env python3

import time
import requests
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
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
SIGNALPLUS_PUSH_TRADE_API = "https://mizar-gateway.signalplus.com/mizar/time-lapse-iv"


def push_plot():
    args = sys.argv[1:]
    currency = args[0].upper()
    iv_type = args[1].lower()
    headers =   {
        "Content-Type" : "application/json"
    }
    now = int(time.time())
    yesterday = (int(time.time()) // (3600*24) - 1) * (3600*24) + 8*3600
    t_minus_7 = (int(time.time()) // (3600*24) - 7) * (3600*24) + 8*3600
    req_body = {
        'type': 'Maturity',
        'ivType':  iv_type,
        'currency': currency,
        'dateList': [now*1000, yesterday*1000, t_minus_7*1000],
        'exchange': 'deribit',
    }
    response = requests.post(SIGNALPLUS_PUSH_TRADE_API, headers=headers, json=req_body)

    rsp_dict = response.json()
    parsed_data = []
    for item in rsp_dict['value']:
        name = item['name']
        x = [point['x'] for point in item['points']]
        y = [float(point['y'])*100 for point in item['points']]
        parsed_data.append({'name': name, 'x': x, 'y': y})
    min_y = parsed_data[0]['y'][0]
    max_y = parsed_data[0]['y'][0]
    if iv_type == 'atm':
        for d in parsed_data:
            y_min = min(d['y'])
            y_max = max(d['y'])
            if y_min < min_y:
                min_y = y_min
            if y_max > max_y:
                max_y = y_max

        for k in range(len(parsed_data)-1):
            parsed_data[k]['y'].insert(4, (parsed_data[k]['y'][3]+parsed_data[k]['y'][4])/2)
        x1 = ['1D', '2D', '1W', '2W', '3W', '1M', '2M', '3M', '6M', '9M', '1Y']
        x2 = ['1W', '2W', '3W', '1M', '2M', '3M', '6M', '9M']
    else:
        for d in parsed_data[:-1]:
            y_min = min(d['y'])
            y_max = max(d['y'])
            if y_min < min_y:
                min_y = y_min
            if y_max > max_y:
                max_y = y_max
        x1 = ['1D', '2D', '1W', '2W', '1M', '2M', '3M', '6M', '9M', '1Y']

    # mpl_style(True)
    plt.rc('xtick', labelsize=10)
    plt.rc('ytick', labelsize=10)

    img = Image.open(f'{assets_dir}/logo.png')
    fig = plt.figure(figsize=(12, 5))
    width, height = fig.get_size_inches()*fig.dpi
    wm_width = int(width/4)
    scaling = (wm_width / float(img.size[0]))
    wm_height = int(float(img.size[1])*float(scaling))
    img = img.resize((wm_width, wm_height), Image.LANCZOS)
    ax = plt.axes()
    xpos = ax.transAxes.transform((0.695,0))[0]
    ypos = ax.transAxes.transform((0,0.805))[1]
    fig.text(0.5, 0.5, 'SignalPlus',
             fontsize=40, color='black',
             ha='center', va='center', alpha=0.1)
    fig.figimage(img, xpos, ypos, alpha=.8, zorder=1)

    plt.plot(x1, parsed_data[2]['y'], label='NOW', marker='o', color='mediumpurple')
    plt.plot(x1, parsed_data[1]['y'], label='T-1', marker='o', color='lightseagreen')
    plt.plot(x1, parsed_data[0]['y'], label='T-7', marker='o', color='steelblue')
    if iv_type == 'atm':
        plt.plot(x2, parsed_data[-1]['y'], '-.', label='RV', marker='o', color='goldenrod')

    plt.ylabel("IV/RV")
    min_y_axis = min_y - (max_y - min_y) / 4
    max_y_axis = max_y + (max_y - min_y) / 4
    plt.yticks(np.arange(min_y_axis, max_y_axis, (max_y_axis-min_y_axis)/4))
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())

    # 创建新的图例
    plt.legend(loc='lower right', fontsize="8")
    title = f'{currency} {iv_type.upper()} Time Lapse IV - Tenor'
    plt.title(title)
    plt.grid(axis='y')

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=fig.dpi)
    buf.seek(0)
    text = f'📊 {title}'
    text += '\n\n'
    if currency == 'BTC':
        text += '<b>🚀 <a href="https://t.signalplus.com">SignalPlus RFQ</a>: Block size liquidity, tightest price. No fees</b>'
    else:
        text += '<b>📈 <a href="https://t.signalplus.com/user/login?redirect=%2Fdashboard">SignalPlus</a>: Advanced options trading with zero fees</b>'
    asyncio.run(bot.send_photo(chat_id=config_yaml["group_chat_id"], photo=buf, caption=text, parse_mode=ParseMode.HTML))


if __name__ == "__main__":
    push_plot()
