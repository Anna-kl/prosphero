from pytrends.request import TrendReq
import sqlalchemy
import datetime
import pandas  as pd
import random
import numpy
import pymysql.cursors
from sqlalchemy import desc
import os

class DB:
    def __init__(self):
        self.con=self.get_param_for_db()


    def get_param_for_db(self):
        url = 'postgresql://{}:{}@{}:5432/{}'
        with open(os.getcwd()+'\settings.txt', 'r') as file:
            data=file.read()
            data=data.split('\n')
            setting = {}
            setting['host'] = data[0].replace('host:', '').replace(' ', '')
            setting['login'] = data[1].replace('login:', '').replace(' ', '')
            setting['password'] = data[2].replace('password:', '').replace(' ', '')
            url=url.format(setting['login'],setting['password'],setting['host'],'prosphero')
            con = sqlalchemy.create_engine(url, echo=True)

            return con
    def return_connection(self):
        return self.con

    def insert(self, mean_d,name_currency,d_currency,temp_k,on_currency ):
        insert = dict(
            currency=name_currency,
            not_cited=d_currency,
            cited=mean_d,
            dttm=temp_k,
            on_currency=on_currency
        )
        cur_insert = currency.insert(insert)
        self.con.execute(cur_insert)

DB_postgres=DB()
meta = sqlalchemy.MetaData(bind=DB_postgres.con, reflect=True, schema='google')
currency = meta.tables['google.currency_normalize']

def get_currency_name():
    result=[]
    url_mysql = 'mysql+mysqldb://reader:nb7hd-1HG6f@clh.datalight.me:3306/coins_dict'
    connection = pymysql.connect(host='clh.datalight.me',
                                 user='reader',
                                 password='nb7hd-1HG6f',
                                 db='coins_dict',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "SELECT source_id, inside_id, symbol FROM coins_dict.coins_source_id where `source`='google_trends';"
            cursor.execute(sql)

            for item in cursor:
                result.append(item)

    except:
        print('error')
    finally:
        connection.close()
    return result

def get_cites(name_currency):
    df1=get_dataframe_from_db()
    pytrend = TrendReq()
    to_list=df1.to_dict()
    for item_name, value in to_list.items():
        pytrend.build_payload(kw_list=[item_name, name_currency], timeframe='today 5-y')
        interest_over_time_df = pytrend.interest_over_time()

        get_d_btc=interest_over_time_df[item_name]
        get_currency = interest_over_time_df[name_currency]
        d_currency = get_currency.to_dict()
        count=0

        for i in range(0, 20):
            key=random.choice(list(d_currency.keys()))
            if d_currency[key]==0:
                    count+=1

        if count>15:
            continue
        d=get_d_btc.to_dict()
        table_currency=currency.select().with_only_columns([currency.c.dttm,currency.c.cited]).where(currency.c.currency==item_name)
        db = DB_postgres.execute(table_currency)
        result_db={}
        for item in db:
            result_db[item[0]]=item[1]

        old_data=datetime.datetime.now()-datetime.timedelta(days=93)
        data_start=datetime.datetime.strptime('2017-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
        for k,n in d.items():
            if k<data_start or k>old_data:
                continue
            if d_currency[k]==0:
                d_currency[k]=0.5
            mean_d=(d_currency[k]*result_db[k])/d[k]
            for i in range(0,7):
                temp_k=k+datetime.timedelta(days=i)
                DB_postgres.insert(mean_d,name_currency,d_currency[k],temp_k,item_name)

        break

def get_data_from_db(name_currency):
    result={}
    table_currency = currency.select().with_only_columns([currency.c.dttm, currency.c.cited]).where(
        currency.c.currency == name_currency)
    db = DB.con.execute(table_currency)
    result_db = {}
    for item in db:
        result_db[item[0]] = item[1]
    return result


def get_cites_from_dttm(name_currency,dttm):
    df1=get_dataframe_from_db()
    pytrend = TrendReq()

    to_list=df1.to_dict()
    for item_name, value in to_list.items():
        pytrend.build_payload(kw_list=[item_name, name_currency], timeframe='today 5-y')
        interest_over_time_df = pytrend.interest_over_time()

        get_d_btc=interest_over_time_df[item_name]
        get_currency = interest_over_time_df[name_currency]
        d_currency = get_currency.to_dict()
        count=0

        for i in range(0, 20):
            key=random.choice(list(d_currency.keys()))
            if d_currency[key]==0:
                    count+=1

        if count>15:
            continue
        d=get_d_btc.to_dict()

        result_db=get_data_from_db(name_currency)
        data_start = dttm
        old_data=datetime.datetime.now()-datetime.timedelta(days=93)

        for k,n in d.items():
            if k<data_start or k>old_data:
                continue
            if d_currency[k]==0:
                d_currency[k]=0.5
            mean_d=(d_currency[k]*result_db[k])/d[k]
            for i in range(0,7):
                temp_k=k+datetime.timedelta(days=i)
                DB_postgres.insert(mean_d, name_currency, d_currency[k], temp_k, item_name)
        break

def get_on_currency(name_currency):
    cursor=currency.select().with_only_columns([currency.c.on_currency]).where(currency.c.currency==name_currency).limit(1)
    db=con.execute(cursor)
    for item in db:
        on_currency=item._row[0]
    return on_currency

def get_not_cited(name_currency, dttm):
    df1 = get_dataframe_from_db()
    pytrend = TrendReq()
    df = get_dataframe_from_db()
    to_list = df1.to_dict()
    item_name=get_on_currency(name_currency)
    pytrend.build_payload(kw_list=[name_currency], timeframe='today 5-y')
    interest_over_time_df = pytrend.interest_over_time()
    get_d_btc = interest_over_time_df[name_currency]
    get_currency = interest_over_time_df[name_currency]
    d_currency = get_currency.to_dict()
    d = get_d_btc.to_dict()
    dttm=datetime.datetime.strptime(dttm,'%Y-%m-%d %H:%M:%S')
    for k, n in d.items():
        if dttm>=k and dttm<k+datetime.timedelta(days=7):

           return get_currency[k]

    return -1


def get_cites_dttm_btc(name_currency, dttm_start, dttm_end):
        pytrend = TrendReq()

        pytrend.build_payload(kw_list=[ 'bitcoin'], timeframe='today 5-y')
        interest_over_time_df = pytrend.interest_over_time()

        get_d_btc = interest_over_time_df['bitcoin']
        get_currency = interest_over_time_df[name_currency]
        d_currency = get_currency.to_dict()

        d = get_d_btc.to_dict()

        for k, n in d.items():


                if k >= dttm_start and k <= dttm_end:

                    mean_d = (d_currency[k] * 1340)/ 100
                    for i in range(0, 7):

                        temp_k = k + datetime.timedelta(days=i)
                        if temp_k>dttm_end:
                            break
                            DB_postgres.insert(mean_d, name_currency, d_currency[k], temp_k, name_currency)


def get_dataframe_from_db():
    name_currency=['bitcoin','ethereum','bch']
    data={}
    max=10000
    for item in name_currency:
        table_currency = currency.select().with_only_columns([currency.c.cited]).where(currency.c.currency==item)
        db=DB.con.execute(table_currency)
        list_currency=list_from_db(db)

        data[item]=list_currency
        if len(list_currency)<max:
            max=len(list_currency)

    for k,i in data.items():
        data[k]=data[k][0:max]
    df=pd.DataFrame(data,columns=name_currency)
    df1=df.mean().sort_values(ascending=False)

    return df1

def get_last_dttm(name_currency):
    cursor=currency.select().with_only_columns([currency.c.dttm]).where(currency.c.currency==name_currency).order_by(desc(currency.c.dttm))
    db=DB.con.execute(cursor)
    for item in db:
        last_dttm=item._row[0]
        break
    return last_dttm

def get_coef_btc(dttm):
    df1 = get_dataframe_from_db()
    pytrend = TrendReq()

    pytrend.build_payload(kw_list=['bitcoin'], timeframe='today 5-y')
    interest_over_time_df = pytrend.interest_over_time()

    get_d_btc = interest_over_time_df['bitcoin']
    get_currency = interest_over_time_df['bitcoin']
    d_currency = get_currency.to_dict()

    d = get_d_btc.to_dict()
    table_currency = currency.select().with_only_columns([currency.c.dttm, currency.c.cited]).where(
        currency.c.currency == 'bitcoin')
    DB.con.execute(table_currency)

    for k, n in d.items():
        if dttm >= k and dttm < k + datetime.timedelta(days=7):
            mean_d = (d_currency[k] * 1340) / 100
            return mean_d

def get_last_90days_btc(name_currency,dttm):
    df1 = get_dataframe_from_db()
    pytrend = TrendReq()


    pytrend.build_payload(kw_list=[name_currency], timeframe='today 3-m')
    interest_over_time_df = pytrend.interest_over_time()

    get_d_btc = interest_over_time_df[name_currency]
    get_currency = interest_over_time_df[name_currency]
    d_currency = get_currency.to_dict()

    d = get_d_btc.to_dict()


    not_cited=get_coef_btc(dttm)
    for k, n in d.items():
        if k>=dttm:
            if d_currency[k] == 0:
                d_currency[k] = 0.5
            coef = not_cited/d_currency[k]
            break
    for k, n in d.items():
                if k >= dttm:
                    mean_d=d_currency[k]*coef
                    DB_postgres.insert(mean_d, name_currency, d_currency[k], k, name_currency)


def get_last_data_btc(name_currency):
    new_data=datetime.datetime.now()
    new_data=datetime.datetime.strptime(str(new_data.year)+'-'+str(new_data.month)+'-'+str(new_data.day)+' 00:00:00', '%Y-%m-%d %H:%M:%S')
    old_data = new_data - datetime.timedelta(days=93)
    last_dttm=get_last_dttm('bitcoin')

    if last_dttm<old_data:
        get_cites_dttm_btc(name_currency, last_dttm,old_data)
    else:
        get_last_90days_btc(name_currency,last_dttm)



def get_cites_dttm(name_currency, last_dttm, old_data):

    pytrend = TrendReq()
    on_currency=get_on_currency(name_currency)

    pytrend.build_payload(kw_list=[on_currency, name_currency], timeframe='today 5-y')
    interest_over_time_df = pytrend.interest_over_time()

    get_d_btc = interest_over_time_df[on_currency]
    get_currency = interest_over_time_df[name_currency]
    d_currency = get_currency.to_dict()
    d = get_d_btc.to_dict()
    result_db=get_data_from_db(name_currency)
    for k, n in d.items():

            if d_currency[k] == 0:
                d_currency[k] = 0.5
            if k >= last_dttm and k <= old_data:

                mean_d = (d_currency[k] * result_db[k]) / d[k]
                for i in range(0, 7):

                    temp_k = k + datetime.timedelta(days=i)
                    if temp_k > old_data:
                        break
                    DB_postgres.insert(mean_d, name_currency, d_currency[k], temp_k, name_currency)


def get_interest( name_currency, param):
    pytrend = TrendReq()
    on_currency = get_on_currency(name_currency)
    pytrend.build_payload(kw_list=[on_currency, name_currency], timeframe=param)
    interest_over_time_df = pytrend.interest_over_time()
    return interest_over_time_df

def get_update_data(name_currency,dttm):
    on_currency = get_on_currency(name_currency)
    interest_over_time_df = get_interest(name_currency,'now 7-d')

    get_d_btc = interest_over_time_df[on_currency]
    get_currency = interest_over_time_df[name_currency]
    d_currency = get_currency.to_dict()
    d = get_d_btc.to_dict()
    table_currency = currency.select().with_only_columns(
        [currency.c.dttm, currency.c.cited, currency.c.not_cited]).where((currency.c.currency == name_currency) & (
                                                                                     currency.c.dttm >= dttm))
    db = DB.con.execute(table_currency)

    for item in db:
        dest = item._row
    sum = 0
    count = 0
    coef = 0
    for k, n in d.items():
        if k >= dttm and k < dttm + datetime.timedelta(days=1) and dttm + datetime.timedelta(
                days=1) < datetime.datetime.now():

            sum += d_currency[k]
            count += 1
        else:
            if sum > 0:
                coef = sum / count
                sum = 0
                count = 0
                coef = dest[1] / coef
                break

    for k, n in d.items():
        if k >= dttm and k < dttm + datetime.timedelta(days=1) and dttm + datetime.timedelta(
                days=1) < datetime.datetime.now():
            sum += d_currency[k]
            count += 1
        else:
            if sum > 0:
                sum = sum / count
                mean_d = sum * coef
                sum = 0
                count = 0
                DB_postgres.insert(mean_d, name_currency, d_currency[k], k, on_currency)

                dttm = dttm + datetime.timedelta(days=1)

def get_7_days(name_currency):
    pytrend = TrendReq()
    on_currency = get_on_currency(name_currency)
    pytrend.build_payload(kw_list=[on_currency,name_currency], timeframe='now 7-d')
    interest_over_time_df = pytrend.interest_over_time()
    old_data=datetime.datetime.now()
    old_data=datetime.datetime.strptime(str(old_data.year)+'-'+str(old_data.month)+'-'+str(old_data.day)+' 00:00:00', '%Y-%m-%d %H:%M:%S')
    old_data=old_data-datetime.timedelta(days=3)

    get_d_btc = interest_over_time_df[on_currency]
    get_currency = interest_over_time_df[name_currency]
    d_currency = get_currency.to_dict()
    d = get_d_btc.to_dict()
    table_currency = currency.select().with_only_columns([currency.c.dttm, currency.c.cited, currency.c.not_cited]).where((
        currency.c.currency == name_currency) & (currency.c.dttm>=old_data))
    db = DB.con.execute(table_currency)

    for item in db:
        dest = item._row
    sum=0
    count=0
    coef=0
    for k, n in d.items():
        if k >= old_data and k<old_data+datetime.timedelta(days=1) and old_data+datetime.timedelta(days=1)<datetime.datetime.now():

            sum += d_currency[k]
            count += 1
        else:
            if sum > 0:
                coef = sum/count
                sum = 0
                count = 0
                coef=dest[1]/coef
                break
    old_data=old_data+datetime.timedelta(days=1)
    for k, n in d.items():
            if k >= old_data and k < old_data + datetime.timedelta(days=1) and old_data + datetime.timedelta(
                    days=1) < datetime.datetime.now():
                sum += d_currency[k]
                count += 1
            else:
                if sum > 0:
                    sum = sum / count
                    mean_d=sum*coef
                    sum = 0
                    count = 0
                    DB_postgres.insert(mean_d, name_currency, d_currency[k], k, on_currency)

                    old_data = old_data + datetime.timedelta(days=1)

def get_last_90days(name_currency, dttm):
    df1 = get_dataframe_from_db()
    pytrend = TrendReq()

    on_currency=get_on_currency(name_currency)
    pytrend.build_payload(kw_list=[on_currency,name_currency], timeframe='today 3-m')
    interest_over_time_df = pytrend.interest_over_time()

    get_d_btc = interest_over_time_df[on_currency]
    get_currency = interest_over_time_df[name_currency]
    d_currency = get_currency.to_dict()

    d = get_d_btc.to_dict()
    result_db=get_data_from_db(name_currency)


    for k, n in d.items():
        if k >= dttm:

            coef = result_db[k]/d_currency[k]
            break
    dttm=dttm+datetime.timedelta(days=1)
    for k, n in d.items():
        if k >= dttm:
            mean_d = d_currency[k] * coef
            DB_postgres.insert(mean_d, name_currency, d_currency[k], k, on_currency)

def get_7_days_btc():
    pytrend = TrendReq()
    pytrend.build_payload(kw_list=['bitcoin'], timeframe='now 7-d')
    interest_over_time_df = pytrend.interest_over_time()
    old_data=datetime.datetime.now()
    old_data=datetime.datetime.strptime(str(old_data.year)+'-'+str(old_data.month)+'-'+str(old_data.day)+' 00:00:00', '%Y-%m-%d %H:%M:%S')
    old_data=old_data-datetime.timedelta(days=3)

    get_d_btc = interest_over_time_df['bitcoin']

    d = get_d_btc.to_dict()
    table_currency = currency.select().with_only_columns([currency.c.dttm, currency.c.cited, currency.c.not_cited]).where((
        currency.c.currency == 'bitcoin') & (currency.c.dttm>=old_data))
    db = DB.con.execute(table_currency)

    for item in db:
        dest = item._row
    sum=0
    count=0
    coef=0
    for k, n in d.items():
        if k >= old_data and k<old_data+datetime.timedelta(days=1) and old_data+datetime.timedelta(days=1)<datetime.datetime.now():

            sum += d[k]
            count += 1
        else:
            if sum > 0:
                coef = sum/count
                sum = 0
                count = 0
                coef=dest[1]/coef
                break
    old_data=old_data+datetime.timedelta(days=1)
    for k, n in d.items():
            if k >= old_data and k < old_data + datetime.timedelta(days=1) and old_data + datetime.timedelta(
                    days=1) < datetime.datetime.now():
                sum += d[k]
                count += 1
            else:
                if sum > 0:
                    sum = sum / count
                    mean_d=sum*coef
                    sum = 0
                    count = 0

                    DB_postgres.insert(mean_d, 'bitcoin', coef, k, 'bitcoin')

                    old_data = old_data + datetime.timedelta(days=1)

def get_last_data(name_currency):
    new_data = datetime.datetime.now()
    new_data = datetime.datetime.strptime(
        str(new_data.year) + '-' + str(new_data.month) + '-' + str(new_data.day) + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
    old_data = new_data - datetime.timedelta(days=93)
    last_dttm = get_last_dttm(name_currency)

    get_cites_dttm(name_currency, last_dttm, old_data)

    get_last_90days(name_currency, last_dttm)
    get_7_days(name_currency)


def list_from_db(db):
    result=[]
    for item in db:
        result.append(item._row[0])
    return result

def check_currency(name_currency):
    table_currency = currency.select().with_only_columns([currency.c.dttm, currency.c.cited]).where(
        currency.c.currency == name_currency)
    db = DB.con.execute(table_currency)
    if db.rowcount>0:
        return False
    else:
        return True

def get_all_history(name_currency):
    get_cites(name_currency)
    get_last_data(name_currency)
