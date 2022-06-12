#get information from website and upload to DB for storage


import pandas as pd
from lxml import etree
import requests
import datetime
from dateutil import *
import pytz
from sqlalchemy import create_engine
import investpy

def generate_todaystring():
    return (datetime.datetime.today()).strftime('%d%m%Y')

def generate_today():
    return (datetime.datetime.today()).date()


#get gold price
url = "https://www.pnj.com.vn/blog/gia-vang/"
html = requests.get(url).content
df_list = pd.read_html(html,flavor='html5lib')

gold_price=df_list[1]
gold_price.columns=['Type','Buy','Sell']
gold_price['Base_date']=generate_todaystring()

from sqlalchemy.dialects.mysql import insert

def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)



#get exchange rate - VCB
import urllib3
import xmltodict

url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
response = requests.get(url)
data = xmltodict.parse(response.content)

short_data=(data['ExrateList']['Exrate'])
currency_code=[]
currency_name=[]
buy=[]
transfer=[]
sell=[]

for item in range(len(short_data)):
    currency_code.append(short_data[item]['@CurrencyCode'])
    currency_name.append(short_data[item]['@CurrencyName'])
    
    if short_data[item]['@Buy']=='-':
        buy_currency=float("nan") #nan for missing value
    else:
        buy_currency=float(short_data[item]['@Buy'].replace(',',''))
    buy.append(buy_currency)
    
    if short_data[item]['@Transfer']=='-':
        transfer_currency=float("nan")
    else:
        transfer_currency=float(short_data[item]['@Transfer'].replace(',',''))    
    transfer.append(transfer_currency)
    
    if short_data[item]['@Sell']=='-':
        sell_currency=float("nan")
    else:
        sell_currency=float(short_data[item]['@Sell'].replace(',',''))     
    sell.append(sell_currency)


 
Exrate_df=pd.DataFrame([currency_code,currency_name,buy,transfer,sell]).transpose()
Exrate_df.columns=['Currency_code','Currency_name','Buy','Transfer','Sell']
Exrate_df['Base_date']=generate_todaystring()


#Upload to MySQL DB
def mysql_engine(user = 'root', password = '123456', host = 'localhost', port = '3306', database = 'world'):
    engine = create_engine("mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(user, password, host, port, database))
    return engine

if generate_todaystring()!=mysql_engine().execute("select max(base_date) from exrate").fetchall()[0][0]:
    Exrate_df.to_sql('exrate',con=mysql_engine(),if_exists='append')
else:
    print('No new data for exrate')
                     
if generate_todaystring()!=mysql_engine().execute("select max(base_date) from goldprice").fetchall()[0][0]:
    gold_price.to_sql('goldprice',con=mysql_engine(),if_exists='append')
else:
    print('No new data for goldprice')


# get vn indices/etfs

#max_date for index table
max_date=mysql_engine().execute("select max(date) from vn_indices").fetchall()[0][0].date()
start = (max_date+ datetime.timedelta(days=1)) .strftime("%d/%m/%Y")
end  = datetime.datetime.now().strftime("%d/%m/%Y")
vn_indices=investpy.get_indices(country='vietnam')



for item in vn_indices['name']:  
    try:
        index_df = investpy.get_index_historical_data(index= item, country= 'vietnam'
            ,from_date = start, to_date = end)
        index_df['Name']=item
        
        max_date=mysql_engine().execute("select max(date) from vn_indices").fetchall()[0][0].date()

        if max_date<generate_today() and max_date<max(index_df.index).date():
        
            index_df.to_sql('vn_indices',con=mysql_engine()
                            ,if_exists='append')
            print('Updated data for '+item+' on '+end)
        
        else:
            print('No new data for '+item+' on '+end)
    except:
        print('No update data for '+item+' on '+end)
        
        
#max_date for etf table
max_date=mysql_engine().execute("select max(date) from vn_etfs").fetchall()[0][0].date()
start = (max_date+ datetime.timedelta(days=1)) .strftime("%d/%m/%Y")
end  = datetime.datetime.now().strftime("%d/%m/%Y")       
vn_etfs=investpy.get_etfs(country='vietnam')
for item in vn_etfs['name']:  
    try:
        index_df = investpy.get_etf_historical_data(etf= item, country= 'vietnam'
            ,from_date = start, to_date = end)
        index_df['Name']=item
        
        max_date=mysql_engine().execute("select max(date) from vn_etfs").fetchall()[0][0].date()
        
        if max_date<generate_today() and max_date<max(index_df.index).date():
        
            index_df.to_sql('vn_etfs',con=mysql_engine()
                            ,if_exists='append')
            print('Updated data for '+item+' on '+end)
        else:
            print('No new data for '+item+' on '+end)
    except:
        print('No update data for '+item+' on '+end)
