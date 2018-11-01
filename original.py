import json
import os
import time
from datetime import datetime
import psycopg2
import requests

e = os.environ

DEBUG = e.get('DEBUG', True)
DONOR = e.get('DONOR', 'https://api.coinmarketcap.com/v1/ticker/?limit=10000')
TABLE_NAME = e.get('TABLE_NAME', 'coinmarketcap.coin')
MAX_QUEUE_LENGTH = e.get('MAX_QUEUE_LENGTH', 10000) # Максимальное количество INSERT в одном запросе
FALSE_VALUE = e.get('FALSE_VALUE', '') # Что отдавать в базу, если значение пустое

SETTINGS_FILE = e.get('SETTINGS_FILE', 'settings.json') # Если нет env переменных, берем данные коннекта отсюда
"""{"login": "", "host": "", "password": "", "db": ""}""" # Что должно быть в этом файле

# Данные для подключения к базе. Используются в первую очередь, если есть.
DB_NAME = e.get('DB_NAME', '')
DB_PASSWORD = e.get('DB_PASSWORD', '')
DB_HOST = e.get('DB_HOST', '')
DB_USER = e.get('DB_USER', '')

# Обязательные поля. Будут пустой строкой, если не было в источнике
required_fields = (
    'max_supply', 'percent_change_7d', 'percent_change_24h', 'percent_change_1h', '24h_volume_usd',
)
required_names = (
    'id','name','symbol'
)
'''
types = {
    ('id', 'name', ):
    '''
# Преобразование названий филдов источника в наши названия
fields_map = {
    '24h_volume_usd': 'h_volume_usd'
}

# Поля, которые нужно исключить
exclude_fields = ('', )

data_update = str(datetime.now())


def get_data(donor):
    r = requests.get(donor)
    if r.status_code != 200:
        raise AssertionError('Something wrong with {}'.format(donor))
    return r.json()


def prepare_data(func):
    def wrap(*args, data):
        # Преобразовываем данные перед отправкой в базу
        # Получаем dict, проверяем на не пустое значение, на обязательное поле и на исключение
        result = dict()
        for k, v in data.items():

            if (k in required_names):
                # Если ключ есть в fields_map -> преобразовываем
                # Фолси значение преобразовываем в наше FALSE_VALUE
                result[k if k not in fields_map else fields_map.get(k)] ='\''+ v+'\'' if v else FALSE_VALUE

            elif (v or (k in required_fields)) and (k not in exclude_fields):
                # Если ключ есть в fields_map -> преобразовываем
                # Фолси значение преобразовываем в наше FALSE_VALUE
                result[k if k not in fields_map else fields_map.get(k)] = v if v else FALSE_VALUE

            # Условия не выполнелись и значение пустое - пропускаем это поле

        result['data_update'] = '\''+data_update+'\''
        if result['symbol']=='\'WIC\'':
            d=0
        return func(*args, result)
    return wrap


class DB:
    def __init__(self):
        self.connect = None
        self.queue = []
        self.cursor = None

    def get_cursor(self):
        if not self.cursor:
            self.cursor = self.get_connect().cursor()

        return self.cursor

    def get_connect(self):
        if not self.connect:
            stop_len = 30
            current_len = 0
            while current_len < stop_len:
                try:
                    conn = psycopg2.connect("dbname='prosphero' user='externalanna'  host='185.230.142.61' password='44iAipyAjHHxkwHgyAPrqPSR5'"
)
                except psycopg2.Error as err:
                    time.sleep(10)
                    print("Connect failed: {}".format(err))
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

    @staticmethod
    def make_query(data):
        if data:
            return '''INSERT INTO {} ({}) VALUES ({} );''' \
                .format(TABLE_NAME, ',  '.join(data.keys()), ', '.join(data.values()))

    @prepare_data
    def push_to_queue(self, data: dict=None):
        query = self.make_query(data)
        if query:
            self.queue.append(query)
            if len(self.queue) == MAX_QUEUE_LENGTH:
                self.make_request()

    def make_request(self):
        while self.queue:
            self.send_query(self.queue.pop())

        if not DEBUG:
            self.commit()
        else:
            print('='*50 + '\nCOMMIT')

    def send_query(self, data):
       ## if not DEBUG:
            self.get_cursor().execute(data)
      ##  else:
         ##   print(data)

    def commit(self):
        self.cursor.commit()

    def close(self):
        if self.queue:
            self.make_request()

        if self.connect:
            self.connect.close()
            self.cursor = None
            self.connect = None


if __name__ == '__main__':
    db = DB()
    try:
        for i in get_data(DONOR):
            db.push_to_queue(data=i)

    finally:
        db.close()
