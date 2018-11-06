from datetime import datetime, timedelta
from string import punctuation
from sqlalchemy import any_
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from langdetect import detect
import os
from nltk.stem.snowball import SnowballStemmer
import sqlalchemy
from nltk.corpus import stopwords

import re

class Settings(object):
    def get_settings(self):
        file =os.getcwd() +'/settings.txt'
        with open(file, 'r', encoding='utf8') as f:
            data = f.read()
            data = data.split('\n')
            setting = {}
            setting['host'] = data[0].replace('host:', '').replace(' ', '')
            setting['login'] = data[1].replace('login:', '').replace(' ', '')
            setting['password'] = data[2].replace('password:', '').replace(' ', '')
            setting['port'] = '5432'
            setting['name'] = 'prosphero'
            f.close()
            return setting


    def __init__(self):
        setting=self.get_settings()
        self.host=setting['host']
        self.login=setting['login']
        self.password=setting['password']
        self.name=setting['name']
        self.port=setting['port']


def get_word_analitics(url):
    word_analitics = []

    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='word_for_search')
    word_table = meta.tables['word_for_search.words']
    word_table = word_table.select()
    db = con.execute(word_table)
    for item in db:
        assessment = dict(
            name=item._row[0].lower(),
            assessment=item._row[1],
            ball=item._row[2]
        )
        word_analitics.append(assessment)
    return word_analitics

def exception_word_read():
    exception_word=[]
    with open('D://exception_word.txt', 'r') as file:
        search_word = file.read()
        search_word = search_word.split('\n')
        for item in search_word:
            exception_word.append(item)

    return exception_word

def get_coin(url):
    word_coin = []
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    word_table = meta.tables['telegram.synonyms']
    word_table = word_table.select().with_only_columns([word_table.c.synonyms])
    db = con.execute(word_table)

    for item in db:

            word=item._row[0].lower().split(',')
            for i in word:
                word_coin.append(i)


    return word_coin

def get_coin_single(symbol,url):
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    word_table = meta.tables['telegram.synonyms']
    word_table = word_table.select().where(word_table.c.symbol==symbol)
    db = con.execute(word_table)
    for item in db:
        assessment = dict(
            symbol=item._row[0].lower(),
            full_name=item._row[1].lower(),
            synonyms=item._row[3].lower()
        )
        return  assessment

def get_coin_list(symbol_list):
    list_coin=[]
    setting = Settings()
    settings_base = Settings.return_settings(setting)
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(settings_base['login'], settings_base['password'], settings_base['host'],
                     settings_base['port'], settings_base['name'])

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    word_table = meta.tables['telegram.synonyms']
    for symbol in symbol_list:
        word_select = word_table.select().where(word_table.c.symbol==symbol)
        db = con.execute(word_select)
        for item in db:
            assessment = dict(
                symbol=item._row[0].lower(),
                full_name=item._row[1].lower(),
                synonyms=item._row[3].lower()
            )
            list_coin.append(assessment)
    return list_coin

def get_tonal_date(args):
    print('start_write - ', str(datetime.now()))
    negativ = ['not', 'no', 't']
    setting = Settings()
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(setting.login, setting.password, setting.host,
                     setting.port, setting.name)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    users_table = meta.tables['telegram.synonyms']
    date_start=args['date_start']
    date_end_temp=date_start+timedelta(days=1)
    flag=True
    w_coin=[]
    foo=[]
    nltk_word = list(stopwords.words('english'))
    word_coin=get_coin_single(args['symbol'],url)
    if word_coin['synonyms']!='':
        temp=word_coin['synonyms'].split(',')
        for j in temp:
            w_coin.append(j)
            foo.append('%'+j+'%')
   ## w_coin.append(word_coin['symbol'])
   ## foo.append('%' + j + '%')

   ## w_coin.append(word_coin['full_name'])
   ## foo.append('%' + word_coin['full_name'] + '%')
    stemmer = SnowballStemmer("english")
    word_analitics = get_word_analitics(url)
    telegram = meta.tables['telegram.message']
    telegram_sql = telegram.select().with_only_columns(
        [telegram.c.message, telegram.c.id_message, telegram.c.name_chat, telegram.c.date]).where(
        (telegram.c.date > args['date_start']) & (telegram.c.date < args['date_end']) & (
            sqlalchemy.func.lower(telegram.c.message).like(any_(foo))))
    db = con.execute(telegram_sql)
    data_word=[]
    coin_word=get_coin(url)
    for item in db:
        data = word_tokenize(item._row[0].lower())
        search_temp = [i for i in w_coin if i in data]
        if search_temp == None:
            continue

        for coin in search_temp:
                    data=[i for i in data if i not in nltk_word]
                    data = [i for i in data if i not in punctuation]
                    data = [i for i in data if re.search('\w',i)]
                    index_coin=data.index(coin)
                    start_search=index_coin-3
                    end_search=index_coin+3
                    if start_search<0:
                        start_search=0
                    if end_search>len(data):
                        end_search=len(data)

                    vector_word=data[start_search:end_search]
                    vector_word=[i for i in vector_word if search_temp[0]!=i]
                    singles = [stemmer.stem(plural) for plural in vector_word]
                    for j in singles:
                        data_word.append(j)
                    print(vector_word)
    fd=nltk.FreqDist(data_word)
    word_coin=fd.most_common()
    fd.plot(150, cumulative=False)



def get_tonal_list(date_start, date_end,symbol):
    print('start_write - ', str(datetime.now()))
    negativ = ['not', 'no', 't']
    setting = Settings()
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(setting.login, setting.password, setting.host,
                     setting.port, setting.name)
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    users_table = meta.tables['telegram.synonyms']
    date_start=date_start
    date_end_temp=date_start+timedelta(days=1)
    flag=True
    w_coin=[]
    word_coin=get_coin_list(symbol)
    coin_search=[]
    for coin in word_coin:
        if coin['synonyms']:
            temp=coin['synonyms'].split(',')
            for j in temp:
                w_coin.append(j)
        w_coin.append(coin['symbol'])
        w_coin.append(coin['full_name'])

    stemmer = SnowballStemmer("english")
    word_analitics=get_word_analitics()
    while flag:
        if date_end_temp>date_end:
            print('end - ',datetime.now())
            break
        flag_sentence=False
        neutral=[]
        telegram = meta.tables['telegram.message']
        telegram_sql = telegram.select().with_only_columns([telegram.c.message,telegram.c.id_message,telegram.c.name_chat,telegram.c.date]).where(
            (telegram.c.date > date_start) & (telegram.c.date < date_end_temp))
        db = con.execute(telegram_sql)
        positively = []
        negative = []

        for item in db:
            flag_sentence=False
            temp_sentence=item._row[0].split('.')
            for sentence in temp_sentence:
                if flag_sentence==True:
                    break
                else:
                    data = word_tokenize(sentence.lower())
                    search_word=[i for i in w_coin if i in data]

            if len(search_word)==0:
                continue
            find_word=','.join(search_word)
            telegram_word = meta.tables['telegram.message_tonal_tf_idf']
            telegram_sql = telegram_word.select().where((telegram_word.c.id == item._row[1])&(telegram_word.c.name_chat==item._row[2]))
            db = con.execute(telegram_sql)
            if db.rowcount > 0 or len(data)==0:
                continue
            singles = [stemmer.stem(plural) for plural in data]
            singles = [i.lower() for i in singles if i not in punctuation]
            search = [i for i in word_analitics if i['name'] in singles]

            if len(search) == 0:
                data_insert=dict(
                    id=item._row[1],
                    symbol=find_word,
                    keyword='',
                    tonal=0,
                    ball=0,
                    name_chat=item._row[2],
                    processing_dttm=item._row[3]
                )
                telegram_sql=telegram_word.insert(data_insert)
                db = con.execute(telegram_sql)
                continue
            rating=0
            keyword=''
            for i in search:
                rating+=i['ball']
                keyword+=i['name']+','
            keyword=keyword[0:len(keyword)-1]
            if rating >= 1:
                index = singles.index(search[0]['name'])
                temp = singles[0:index]
                temp_negative = [i for i in temp if i in negativ]
                if len(temp_negative) > 0:
                    tonal=-1
                    ball=(rating)*(-1)
                else:
                    tonal=1
                    ball = rating
            elif rating <= -1:
                index = singles.index(search[0]['name'])
                temp = singles[0:index]
                temp_negative = [i for i in temp if i in negativ]
                if len(temp_negative) > 0:
                    tonal=1
                    ball=(rating)*(-1)
                else:
                   tonal=-1
                   ball=rating
            else:
                tonal=0
                rating=0
            data_insert = dict(
                id=item._row[1],
                symbol=find_word,
                keyword=keyword,
                tonal=tonal,
                ball=rating,
                name_chat=item._row[2],
                processing_dttm=item._row[3]
            )
            telegram_sql = telegram_word.insert(data_insert)
            db = con.execute(telegram_sql)

        date_start=date_start+timedelta(days=1)
        date_end_temp=date_start+timedelta(days=1)

def get_tonal(date_start, date_end,symbol):
    print('start_write')
    negativ = ['not', 'no', 't']
    setting = Settings()
    settings_base = Settings.return_settings(setting)
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(settings_base['login'], settings_base['password'], settings_base['host'],
                     settings_base['port'], settings_base['name'])

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    users_table = meta.tables['telegram.synonyms']
    try:
        date_start=datetime.strptime(date_start,'%Y-%m-%d_%H:%M:%S')
    except:
        print('wrong format date_start')
        return -1
    try:
        date_start = datetime.strptime(date_end, '%Y-%m-%d_%H:%M:%S')
    except:
        print('wrong format date_end')

    date_end_temp=date_start+timedelta(days=1)
    flag=True
    w_coin=[]
    word_coin=get_coin_single(symbol)
    if word_coin['synonyms']!='':
        temp=word_coin['synonyms'].split(',')
        for j in temp:
            w_coin.append(j)
   ## w_coin.append(word_coin['symbol'])
    w_coin.append(word_coin['full_name'])
    stemmer = SnowballStemmer("english")
    word_analitics=get_word_analitics()
    while flag:
        flag_sentence=False
        neutral=[]
        telegram = meta.tables['telegram.message']
        telegram_sql = telegram.select().with_only_columns([telegram.c.message,telegram.c.id_message,telegram.c.name_chat,telegram.c.date]).where(
            (telegram.c.date > date_start) & (telegram.c.date < date_end_temp))
        db = con.execute(telegram_sql)
        positively = []
        negative = []

        for item in db:
            flag_sentence=False
            temp_sentence=item._row[0].split('.')
            for sentence in temp_sentence:

                if flag_sentence==True:
                    break
                else:
                    data = word_tokenize(sentence.lower())
                for pattern in w_coin:
                    temp_pattern=pattern.split(' ')
                    if len(temp_pattern)>1:
                        if re.search(pattern,sentence):
                            flag_sentence=True
                            break
                    else:
                        if pattern in data:
                            flag_sentence=True
                            break

            if flag_sentence==False:
                continue
            telegram_word = meta.tables['telegram.message_tonal_tf_idf']
            telegram_sql = telegram_word.select().where((telegram_word.c.id == item._row[1])&(telegram_word.c.name_chat==item._row[2]))
            db = con.execute(telegram_sql)
            if db.rowcount > 0 or len(data)==0:
                continue

            singles = [stemmer.stem(plural) for plural in data]
            singles = [i.lower() for i in singles if i not in punctuation]
            search = [i for i in word_analitics if i['name'] in singles]
            if len(search) == 0:
                data_insert=dict(
                    id=item._row[1],
                    symbol=symbol,
                    keyword='',
                    tonal=0,
                    ball=0,
                    name_chat=item._row[2],
                    processing_dttm=item._row[3]
                )
                telegram_sql=telegram_word.insert(data_insert)
                db = con.execute(telegram_sql)
                continue
            rating=0
            keyword=''
            for i in search:
                rating+=i['ball']
                keyword+=i['name']+','
            keyword=keyword[0:len(keyword)-1]
            if rating >= 1:
                index = singles.index(search[0]['name'])
                temp = singles[0:index]
                temp_negative = [i for i in temp if i in negativ]
                if len(temp_negative) > 0:
                    tonal=-1
                    ball=(rating)*(-1)

                else:
                    tonal=1
                    ball = rating
            elif rating <= -1:
                index = singles.index(search[0]['name'])
                temp = singles[0:index]
                temp_negative = [i for i in temp if i in negativ]
                if len(temp_negative) > 0:
                    tonal=1
                    ball=(rating)*(-1)
                else:
                   tonal=-1
                   ball=rating
            else:
                tonal=0
                rating=0
            data_insert = dict(
                id=item._row[1],
                symbol=symbol,
                keyword=keyword,
                tonal=tonal,
                ball=rating,
                name_chat=item._row[2],
                processing_dttm=item._row[3]
            )
            telegram_sql = telegram_word.insert(data_insert)
            db = con.execute(telegram_sql)


def asynchronous():
    last_date=datetime.now().date()
    date_end= datetime.strptime(str(last_date.year)+'-'+str(last_date.month)+'-'+str(last_date.day)+' 00:00:00', '%Y-%m-%d %H:%M:%S')
    date_start=date_end-timedelta(days=1)
    setting = Settings()

    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(setting.login, setting.password, setting.host,
                     setting.port, setting.name)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    users_table = meta.tables['telegram.synonyms']
    coin_start=0
    while True:
        cursor=users_table.select().with_only_columns([users_table.c.symbol]).where((users_table.c.rank_coin>=coin_start)&(users_table.c.rank_coin<=coin_start+5))
        db = con.execute(cursor)
        if db.rowcount==0:
            break
        task = []
        for item in db:
            if type(item._row)=='tuple':
                symbol=item._row[0][0]
            else:
                symbol = item._row[0]
            item=dict(
                date_start=date_start,
                date_end=date_end,
                symbol=symbol
            )
            task.append(item)
        for i in task:
            get_tonal_date(i)

        coin_start+=5

    print('date_end ' + str(datetime.now()))

def asynchronous_date():
    date_start = datetime.strptime('2018-06-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    date_end = datetime.strptime('2018-11-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    setting = Settings()

    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(setting.login, setting.password, setting.host,
                     setting.port, setting.name)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, echo=True)
    meta = sqlalchemy.MetaData(bind=con, reflect=True, schema='telegram')
    users_table = meta.tables['telegram.synonyms']

    cursor=users_table.select().with_only_columns([users_table.c.symbol]).where((users_table.c.rank_coin>0)&(users_table.c.rank_coin<3))
    db = con.execute(cursor)

   ## list_coin=['eth','btc']
    task = []
    for item in db:

        if type(item._row)=='tuple':
            symbol=item._row[0][0]
        else:
            symbol = item._row[0]

        item=dict(
            date_start=date_start,
            date_end=date_end,
            symbol=symbol
        )
        task.append(item)

    for i in task:
        get_tonal_date(i)

asynchronous_date()
'''
symbol="btc"
date_start=datetime.strptime('2018-10-16 00:00:00','%Y-%m-%d %H:%M:%S')
date_end=date_start+timedelta(days=1)



asynchronous_date()

symbol="btc"
date_start=datetime.strptime('2018-10-16 00:00:00','%Y-%m-%d %H:%M:%S')
date_end=date_start+timedelta(days=1)

get_tonal_list(date_start,date_end,['btc','eth','xrp','bch','eos'])

if __name__ == '__main__':
    print('start')
    print(sys.argv[1:])
    date_start=sys.argv[1].split('_')

    get_tonal(sys.argv[1],sys.argv[2],sys.argv[3])

'''
