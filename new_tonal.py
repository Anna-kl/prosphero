import json
import os
import time
from datetime import datetime
import psycopg2
import requests
import sqlalchemy
import gevent

e = os.environ

DEBUG = e.get('DEBUG', True)
DONOR = e.get('DONOR', 'https://api.coinmarketcap.com/v1/ticker/?limit=10000')
SCHEMA=e.get('SCHEMA','telegram')
SCHEMA_WORD=e.get('SCHEMA','word_for_search')
TABLE_TELEGRAM = e.get('TABLE_NAME','telegram.message')
TABLE_COIN = e.get('TABLE_NAME','synonyms')
TABLE_WORD = e.get('TABLE_NAME','word_for_search.words')
MAX_QUEUE_LENGTH = e.get('MAX_QUEUE_LENGTH', 10000) # Максимальное количество INSERT в одном запросе
FALSE_VALUE = e.get('FALSE_VALUE', '') # Что отдавать в базу, если значение пустое

SETTINGS_FILE = e.get('SETTINGS_FILE', 'settings.json') # Если нет env переменных, берем данные коннекта отсюда
"""{"login": "", "host": "", "password": "", "db": ""}""" # Что должно быть в этом файле

# Данные для подключения к базе. Используются в первую очередь, если есть.
DB_NAME = e.get('DB_NAME', 'prosphero')
DB_PASSWORD = e.get('DB_PASSWORD', '44iAipyAjHHxkwHgyAPrqPSR5')
DB_HOST = e.get('DB_HOST', '185.230.142.61')
DB_USER = e.get('DB_USER', 'externalanna')
DB_PORT = e.get('DB_PORT', 'externalanna')

class DB:
    def __init__(self):
        self.connect = None
        self.queue = []
        self.cursor = None
        self.url = None


    def get_cursor(self):
        if not self.cursor:
            self.cursor = self.get_connect().cursor()

        return self.cursor
    def get_url(self):
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(DB_USER, DB_PASSWORD, DB_HOST,DB_PORT
                         , DB_NAME)
        return self.url

    def get_connect(self):
        if not self.connect:
            stop_len = 30
            current_len = 0
            while current_len < stop_len:
                try:
                    conn = sqlalchemy.create_engine(self.get_url(), echo=True)
                except:
                    time.sleep(10)
                    print("Connect failed")
                    current_len += 1
                    continue

                self.connect = conn
                break

            else:
                raise AssertionError("Can't connect to base")

        return self.connect

    def gen_credentials_str(self):
        return "dbname='{db}' user='{login}' host='{host}' password='{password}'".format(**self.get_credentials())

    @staticmethod
    def get_credentials():
        if all([DB_NAME, DB_PASSWORD, DB_HOST, DB_USER]):
            return dict(
                db=DB_NAME,
                login=DB_USER,
                host=DB_HOST,
                password=DB_PASSWORD
            )
        try:
            credentials = open(SETTINGS_FILE)
        except IOError as e:
            raise AssertionError("{} file load problems {}".format(SETTINGS_FILE, e))
        return json.load(credentials)



    def close(self):
        if self.queue:
            self.make_request()

        if self.connect:
            self.connect.close()
            self.cursor = None
            self.connect = None

class Message():
    def __init__(self):
        self.message=[]

    def get_message(self,date_start,date_end):
        db=DB()
        con = DB.get_connect(DB)
        meta = sqlalchemy.MetaData(bind=con, reflect=True, schema=SCHEMA)
        telegram = meta.tables[TABLE_TELEGRAM]
        telegram_sql = telegram.select().with_only_columns(
            [telegram.c.message, telegram.c.id_message, telegram.c.name_chat, telegram.c.date]).where(
            (telegram.c.date > date_start) & (telegram.c.date < date_end) & (
            sqlalchemy.func.lower(telegram.c.message).like(any_(foo))))
        db = con.execute(telegram_sql)


class Word:
    def __init__(self):
        self.word_analitics=[]
        self.stop_word=['not', 'no', 't']
        self.w_coin=[]


    def get_word(self):
        db=DB()
        con=DB.get_connect(DB)
        meta=sqlalchemy.MetaData(bind=con, reflect=True, schema=SCHEMA_WORD)
        word_table = meta.tables[TABLE_WORD]
        cursor=word_table.select()
        db = con.execute(word_table)
        for item in db:
            assessment = dict(
                name=item._row[0].lower(),
                assessment=item._row[1],
                ball=item._row[2]
            )
            self.word_analitics.append(assessment)
        return self.word_analitics.append

    def return_stop_word(self):
        return self.stop_word


class Coin():
    def __init__(self):
        self.coin=[]
        self.task=[]

    def get_data(self, item):
        for i in item:
            coin_temp = []
            sinonium = []
            foo = []
            if type(item._row) == 'tuple':
                temp_word=item._row[0][2].split(',')
                for temp_i in temp_word:

                    sinonium.append(temp_i)
                    foo.append('%'+temp_i+'%')

                coin_data = dict(
                    symbol=item._row[0][0],
                    sinonium=sinonium,
                    foo=foo
                )
            else:

                temp_word = item._row[0][2].split(',')
                for temp_i in temp_word:
                    sinonium.append(temp_i)
                    foo.append('%' + temp_i + '%')

                coin_data = dict(
                    symbol=item._row[0],
                    sinonium=sinonium,
                    foo=foo
                )
            self.coin.append(coin_data)
        return self.coin

    def get_coin(self, coin_start):
        db = DB()
        con = DB.get_connect(DB)
        meta = sqlalchemy.MetaData(bind=con, reflect=True, schema=TABLE_TELEGRAM)
        telegram_table=meta.tables['telegram.synonyms']
        cursor = telegram_table.select().with_only_columns([telegram_table.c.symbol]).where(
            (telegram_table.c.rank_coin >= coin_start) & (telegram_table.c.rank_coin <= coin_start + 5))
        db = con.execute(cursor)
        if db.rowcount == 0:
            return -1

        for item in db:

            if type(item._row) == 'tuple':
                sinonium=[]
                for i in
                coin_temp=dict(
                symbol = item._row[0][0]

                )
            else:
                symbol = item._row[0]

            self.coin.append(item)
        db.close()
        return self.coin

    def get_task(self,date_start, date_end,coin_start):
        self.get_coin(coin_start)
        for item in self.coin:
            item_current = dict(
                date_start=date_start,
                date_end=date_end,
                symbol=item
            )
            self.task.append(item_current)
        return self





def return_task(coin_start,date_delta):
    coin=Coin()
    task=coin.get_task(date_delta['date_start'], date_delta['date_end'], coin_start)

def get_datetime(date_start,date_end):
    date_start = datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S')
    date_end = datetime.strptime(date_end, '%Y-%m-%d %H:%M:%S')
    time_delta=dict (
        date_start=date_start,
        date_end=date_end
    )
    return time_delta

def insert_tonal_statistic():
    coin_start=0
    date_delta=get_datetime()
    while coin_start<100:
        task=return_task(coin_start,date_delta)
        threads = [gevent.spawn(get_tonal_date, i) for i in task.task]
        gevent.joinall(threads)
        coin_start += 5


