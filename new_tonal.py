import json
import os
import time
import psycopg2
from string import punctuation
import requests
import sqlalchemy
import gevent
from string import punctuation
from nltk.stem.snowball import SnowballStemmer
from sqlalchemy import any_
from nltk.tokenize import sent_tokenize, word_tokenize
from datetime import datetime

e = os.environ

DEBUG = e.get('DEBUG', True)
DONOR = e.get('DONOR', 'https://api.coinmarketcap.com/v1/ticker/?limit=10000')
SCHEMA=e.get('SCHEMA','telegram')
SCHEMA_WORD=e.get('SCHEMA','word_for_search')
TABLE_TELEGRAM = e.get('TABLE_NAME','telegram.message')
TABLE_COIN = e.get('TABLE_NAME','synonyms')
TABLE_WORD = e.get('TABLE_NAME','word_for_search.words')
TABLE_SYNONYM=e.get('TABLE_SYNONYM','telegram.synonyms')
TABLE_INSERT=e.get('TABLE_INSERT','telegram.message_tonal_tf_idf')
MAX_QUEUE_LENGTH = e.get('MAX_QUEUE_LENGTH', 10000) # Максимальное количество INSERT в одном запросе
FALSE_VALUE = e.get('FALSE_VALUE', '') # Что отдавать в базу, если значение пустое

SETTINGS_FILE = e.get('SETTINGS_FILE', 'settings.json') # Если нет env переменных, берем данные коннекта отсюда
"""{"login": "", "host": "", "password": "", "db": ""}""" # Что должно быть в этом файле

# Данные для подключения к базе. Используются в первую очередь, если есть.
DB_NAME = e.get('DB_NAME', 'prosphero')
DB_PASSWORD = e.get('DB_PASSWORD', '44iAipyAjHHxkwHgyAPrqPSR5')
DB_HOST = e.get('DB_HOST', '185.230.142.61')
DB_USER = e.get('DB_USER', 'externalanna')
DB_PORT = e.get('DB_PORT', '5432')

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
        self.url = url.format(DB_USER, DB_PASSWORD, DB_HOST,DB_PORT
                         , DB_NAME)
        return self.url

    def get_connect(self):
        if not self.connect:
            stop_len = 30
            current_len = 0
            while current_len < stop_len:
                try:
                    url=self.get_url()
                    conn = sqlalchemy.create_engine(url, echo=True)
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

    def get_message(self,date_start,date_end, foo):
        db=DB()
        con = DB.get_connect(db)
        meta = sqlalchemy.MetaData(bind=con, reflect=True, schema=SCHEMA)
        telegram = meta.tables[TABLE_TELEGRAM]

        telegram_sql = telegram.select().with_only_columns(
            [telegram.c.message, telegram.c.id_message, telegram.c.name_chat, telegram.c.date]).where(
            (telegram.c.date > date_start) & (telegram.c.date < date_end) & (
            sqlalchemy.func.lower(telegram.c.message).like(any_(foo))))
        db = con.execute(telegram_sql)
        for item in db:
            if self.check_message(item._row[1]):
                message_dict=dict(
                    message=item._row[0].lower(),
                    id_message=item._row[1],
                    name_chat=item._row[2],
                    date=item._row[3]
                )
                self.message.append(message_dict)
        db.close()
        return  self.message

    def check_message(self,id):
        db=DB()
        con = DB.get_connect(db)
        meta = sqlalchemy.MetaData(bind=con, reflect=True, schema=SCHEMA)
        telegram = meta.tables[TABLE_INSERT]
        cusor=telegram.select().where(telegram.c.id==id)
        data=con.execute(cusor)
        db.close()
        if data.rowcount==0:
            return True

def list_data(*args):

    insert_item=dict(
        id=args,
        symbol=args,
        title=args,
        key_coin=args,
        keyword=args
    )
    return insert_item



class Word:
    def __init__(self):
        self.word_analitics=[]
        self.stop_word=['not', 'no', 't']



    def get_word(self):
        db=DB()
        con=DB.get_connect(db)
        meta=sqlalchemy.MetaData(bind=con, reflect=True, schema=SCHEMA_WORD)
        word_table = meta.tables[TABLE_WORD]
        cursor=word_table.select()
        db = con.execute(cursor)
        for item in db:
            assessment = dict(
                name=item._row[0].lower(),
                assessment=item._row[1],
                ball=item._row[2]
            )
            self.word_analitics.append(assessment)
        return self.word_analitics

    def return_stop_word(self):
        return self.stop_word


class Coin():
    def __init__(self):
        wb=Word()
        self.coin=self.get_data()
        self.task=[]
        self.stop_word=wb.stop_word
        self.word=wb.get_word()



    def get_data(self):
        db=DB()
        coin=[]
        con = DB.get_connect(db)
        meta = sqlalchemy.MetaData(bind=con, reflect=True, schema=SCHEMA)
        word_table = meta.tables[TABLE_SYNONYM]
        synonym=word_table.select()
        db=con.execute(synonym)
        for item in db:
            coin_temp = []
            sinonium = []
            foo = []
            if type(item._row) == 'tuple':
                temp_word=item._row[0][3].split(',')
                for temp_i in temp_word:

                    sinonium.append(temp_i.lower())
                    foo.append('%'+temp_i.lower()+'%')

                coin_data = dict(
                    symbol=item._row[0][0],
                    sinonium=sinonium,
                    foo=foo
                )
            else:

                temp_word = item._row[3].split(',')
                for temp_i in temp_word:
                    sinonium.append(temp_i.lower())
                    foo.append('%' + temp_i.lower() + '%')

                coin_data = dict(
                    symbol=item._row[0],
                    sinonium=sinonium,
                    foo=foo,
                    title=item._row[1]
                )

            coin.append(coin_data)
        db.close()
        return coin

    def get_coin_foo(self, symbol):
        try:
            symbol=[i for i in self.coin if i['symbol']==symbol]
        except:
            return -1
        return  symbol[0]



    def get_symbol(self, item):
        for i in item:
            coin_temp = []
            sinonium = []
            foo = []
            if type(item._row) == 'tuple':
                temp_word=item._row[0][2].split(',')
                for temp_i in temp_word:

                    sinonium.append(temp_i)
                    foo.append('%'+temp_i.lower()+'%')

                coin_data = dict(
                    symbol=item._row[0][0],
                    sinonium=sinonium,
                    foo=foo
                )
            else:

                temp_word = item._row[0][2].split(',')
                for temp_i in temp_word:
                    sinonium.append(temp_i)
                    foo.append('%' + temp_i.lower() + '%')

                coin_data = dict(
                    symbol=item._row[0],
                    sinonium=sinonium,
                    foo=foo
                )
            self.coin.append(coin_data)
        return self.coin

coin = Coin()
stemmer = SnowballStemmer("english")

'''
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
'''

def get_task(date_start, date_end, coin_insert):


            item_current = dict(
                date_start=date_start,
                date_end=date_end,
                symbol=coin_insert,
                title=coin.get_coin_foo(coin_insert)['title']

            )

            return item_current



def return_task(coin_start,date_delta, delta, coin):
    task=[]
    for i in range (coin_start,coin_start+delta):
        task.append(get_task(date_delta['date_start'], date_delta['date_end'], coin[i]['symbol']))
    return task


def get_datetime(date_start,date_end):
    date_start = datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S')
    date_end = datetime.strptime(date_end, '%Y-%m-%d %H:%M:%S')
    time_delta=dict (
        date_start=date_start,
        date_end=date_end
    )
    return time_delta

def insert_tonal_statistic(date_start,date_end):
    coin_start=0
    date_delta=get_datetime(date_start,date_end
                        )

    delta=5
    while coin_start<100:

        task=return_task(coin_start,date_delta, delta, coin.coin)
        '''
        for i in task:
            get_tonal_date(i)
        '''
        threads = [gevent.spawn(get_tonal_date, i) for i in task]
        gevent.joinall(threads)
        
        coin_start += delta

def get_rating(search, singles):
    rating = 0
    keyword = ''
    for i in search:
        rating += i['ball']
        keyword += i['name'] + ','
    keyword = keyword[0:len(keyword) - 1]
    if rating >= 1:
        index = singles.index(search[0]['name'])
        temp = singles[0:index]
        temp_negative = [i for i in temp if i in coin.stop_word]
        if len(temp_negative) > 0:
            tonal = -1
            ball = (rating) * (-1)

        else:
            tonal = 1
            ball = rating
    elif rating <= -1:
        index = singles.index(search[0]['name'])
        temp = singles[0:index]
        temp_negative = [i for i in temp if i in coin.stop_word]
        if len(temp_negative) > 0:
            tonal = 1
            ball = (rating) * (-1)
        else:
            tonal = -1
            ball = rating
    else:
        tonal = 0
        rating = 0

    rating_dict=dict(
        tonal=tonal,
        rating=rating,
        keyword=keyword
    )
    return rating_dict


def get_tonal_date(args):
    print('start_write - ', str(datetime.now()))
    foo_words=coin.get_coin_foo(args['symbol'])
    message=Message()
    message.get_message(args['date_start'],args['date_end'],foo_words['foo'])
    telegram_sql=[]
    for item in message.message:
        data = word_tokenize(item['message'])
        search_temp = [i for i in foo_words['sinonium'] if i in data]
        if len(search_temp) == 0:
            continue


        temp_sentence = item['message'].split('.')
        for sentence in temp_sentence:

            data = word_tokenize(sentence)
            search_temp = [i for i in foo_words['sinonium'] if i in data]
            if len(search_temp) == 0:
                continue
            singles = [stemmer.stem(plural) for plural in data]
            singles = [i for i in singles if i not in punctuation]
            search = [i for i in coin.word if i['name'] in singles]

            if len(search) == 0:
                data_insert = dict(
                    id=item['id_message'],
                    symbol=args['symbol'],
                    title=args['title'],
                    key_coin=search_temp[0],
                    keyword='',
                    tonal=0,
                    ball=0,
                    name_chat=item['name_chat'],
                    processing_dttm=item['date']
                )

                telegram_sql.append(data_insert)
                continue

            rating=get_rating(search,singles)
            data_insert = dict(
                id=item['id_message'],
                symbol=args['symbol'],
                keyword=rating['keyword'],
                title=args['title'],
                key_coin=search_temp[0],
                tonal=rating['tonal'],
                ball=rating['rating'],
                name_chat=item['name_chat'],
                processing_dttm=item['date']
            )
            telegram_sql.append(data_insert)

    insert_in_DB(telegram_sql)

def insert_in_DB(telegram_sql):
    db=DB()
    con = DB.get_connect(db)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema=SCHEMA)
    telegram = meta.tables[TABLE_INSERT]

    cursor=telegram.insert(telegram_sql)
    db.connect.execute(cursor)


insert_tonal_statistic('2018-10-01 00:00:00','2018-11-01 00:00:00')
