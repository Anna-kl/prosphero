
import sqlalchemy
import datetime

import random
import pymysql.cursors
from sqlalchemy import desc
import function_trends





def get_all_currency():
    name_currency=function_trends.get_currency_name()

    for item in name_currency:
        if item['source_id'].lower().replace('$','').replace('#','')==item['symbol'].lower().replace('$','').replace('#',''):
            if function_trends.check_currency(item['symbol'].lower().replace('$','').replace('#','')):
                function_trends.get_all_history(item['symbol'].lower().replace('$','').replace('#',''))
        else:
            if function_trends.check_currency(item['symbol'].lower().replace('$','').replace('#','')):
                function_trends.get_all_history(item['symbol'].lower().replace('$','').replace('#',''))
            if function_trends.check_currency(item['source_id'].lower().replace('$','').replace('#','')):
                function_trends.get_all_history(item['source_id'].lower().replace('$','').replace('#',''))


def get_currency(name_currency):
    dttm=function_trends.get_last_data(name_currency)
    if dttm<datetime.datetime.now()-datetime.timedelta(days=93):
        function_trends.get_cites_from_dttm(name_currency,dttm)
        dttm=function_trends.get_last_data(name_currency)
        function_trends.get_last_90days(name_currency,dttm)
        function_trends.get_7_days(name_currency)
    elif dttm<datetime.datetime.now()-datetime.timedelta(days=7):
        function_trends.get_last_90days(name_currency, dttm)
        function_trends.get_7_days(name_currency)
    else:
        function_trends.get_7_days(name_currency)

def update():
    currency_all=function_trends.get_currency_name()
    for item in currency_all:
        if item['source_id'].lower() == item['symbol'].lower():
            if function_trends.check_currency(item['symbol'].lower()):
                function_trends.get_update_data(item['symbol'].lower())
        else:
            if function_trends.check_currency(item['symbol'].lower()):
                function_trends.get_update_data(item['symbol'].lower())
            if function_trends.check_currency(item['source_id'].lower()):
                function_trends.get_update_data(item['source_id'].lower())

function_trends.get_cites_dttm_btc('bitcoin','2017-01-01 00:00:00','2018-11-11 00:00:00')
