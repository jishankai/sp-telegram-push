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

assets_dir = Path(__file__).parent.parent.resolve() / "assets"
SIGNALPLUS_EXPIRE_IV_API = "https://mizar-gateway.signalplus.com/mizar/expire-lapse-iv"
plt.rcParams['font.family'] = 'monospace'


async def push_iv():
    args = sys.argv[1:]
    currency = args[0].upper()
    exchange_name = args[1].lower()
    headers =   {
        "Content-Type" : "application/json"
    }
    req_body = {
        "currency": currency,
        "exchange": exchange_name,
    }
    response = requests.post(SIGNALPLUS_EXPIRE_IV_API, headers=headers, json=req_body)
    json_data = response.json()
    data = [['Tenor', 'Future', '10P','25P', 'ATMF', '25C', '10C', '10D FLY', '25D FLY', '10D RR', '25D RR']]
    for i,item in enumerate(json_data['value']):
        data.append([ item['tenor'], 
        convert_to_float(item['future']),
        convert_to_percentage(item['put10p']),
        convert_to_percentage(item['put25p']),
        convert_to_percentage(item['atm']),
        convert_to_percentage(item['call25p']),
        convert_to_percentage(item['call10p']),
        convert_to_percentage(item['fly10p']),
        convert_to_percentage(item['fly25p']),
        convert_to_percentage(item['rr10p']),
        convert_to_percentage(item['rr25p'])
        ])

    # 创建新的图例
    title_text = f'{currency} {exchange_name.upper()} Volatility Table'
    now = datetime.datetime.utcnow()
    footer_text = now.strftime('%Y-%m-%d %H:%M UTC+0')
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
                          colWidths=[0.15] + [1 / len(column_headers)] * (len(column_headers) - 1), # 根据列数自动调整列宽
                          colColours=ccolors,
                          colLabels=column_headers,
                          loc='center')
    for key, cell in the_table.get_celld().items():
        cell.set_text_props(fontsize='xx-large')
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

    plt.savefig('test.png', format='png', dpi=fig.dpi)

def convert_to_percentage(s):
    # 将字符串转换为浮点数并乘以100
    num = float(s) * 100
    # 格式化为百分数并添加百分号
    return f"{num:.2f}%"
def convert_to_float(s):
    # 将字符串转换为浮点数
    num = float(s)
    # 格式化为百分数并添加百分号
    return f"{num:.2f}"

if __name__ == "__main__":
    asyncio.run(push_iv())
