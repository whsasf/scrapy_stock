# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import urllib.request
import urllib.parse
from pymongo import MongoClient
from scrapy import Item
from scrapy.conf import settings
import datetime

class ScrapyStockPipeline(object):
    def process_item(self, item, spider):
        return item

class Prepare(object):
    """
    this class is used to provide some seperate work
    """

    @staticmethod
    def fetch_rate():
        """
        this function is used to fetch rate of HKY to RMB and dollar to RMB
        """
        url = 'http://web.juhe.cn:8080/finance/exchange/rmbquot?key=27457fb2446aeeb661161f4138e9c597'
        try:
            data_stream = eval(urllib.request.urlopen(url,None,10).read().decode('utf-8').replace('null','"null"'))
            if data_stream['resultcode'] == "200":
                #print(data_stream['result'][''])
                for data in data_stream['result'][0]:

                    if data_stream['result'][0][data]['name'] == '美元':
                        us2rmb_rate = float(data_stream['result'][0][data]['mSellPri'])/100
                    elif data_stream['result'][0][data]['name'] == '港币':
                        hk2rmb_rate = float(data_stream['result'][0][data]['mSellPri'])/100
            else:
                return ('error')
        except:
            return ('error')
        if float(us2rmb_rate) >0 and float(hk2rmb_rate) >0 :
            return (us2rmb_rate,hk2rmb_rate)
        else:
            return ('error')

    @staticmethod
    def change_mongo_status(s1,s2):
        status1 = s1
        status2 = s2
        db_uri = settings.get('MONGODB_URI','mongodb://localhost:27017')
        db_name = settings.get('MONGODB_NAME','stockdb')

        db_client = MongoClient(db_uri)
        db = db_client[db_name]

        try:
            collection = db['stock_latest']
        except:
            return ('error')
        else:
            collection.update_many({"status_flag": status1},{"$set":{"status_flag": status2}},True)
        finally:
            db_client.close()


    @staticmethod
    def write_mongo_accessory_collecton(**kw):
        db_uri = settings.get('MONGODB_URI','mongodb://localhost:27017')
        db_name = settings.get('MONGODB_NAME','stockdb')
        db_client = MongoClient(db_uri)
        db = db_client[db_name]

        try:
            collection = db['accessory']
            for key ,value in kw.items():
                collection.update_one({},{"$set":{key:value}},True)
        except:
            return ('error')
        finally:
            db_client.close()


    @staticmethod
    def read_mongo_accessory_collecton(*key_list):
        db_uri = settings.get('MONGODB_URI','mongodb://localhost:27017')
        db_name = settings.get('MONGODB_NAME','stockdb')
        db_client = MongoClient(db_uri)
        db = db_client[db_name]
        result = []
        try:
            collection = db['accessory']
            response = dict(list(collection.find({},{"_id":0}))[0])
            for key in key_list:
                if float(response[key]) > 0:
                    result.append(response[key]) 
                else:
                    db_client.close()
                    return ('error')
        except:
            return ('error')
        finally:
            db_client.close()
        return (result)


class Foreignmoney2rmyPipeline(object):
    
    #hk2rmb_rate = 0.8952
    #us2rmb_rate = 7.0079
    oo = Prepare.fetch_rate()
    if oo != 'error' and len(oo) == 2 : #success
        print('fetching rate value successfully!!')
        us2rmb_rate,hk2rmb_rate = oo
        Prepare.write_mongo_accessory_collecton(us2rmb_rate=us2rmb_rate,hk2rmb_rate=hk2rmb_rate)
    else:
        print('reading rate value form database!')
        array = Prepare.read_mongo_accessory_collecton('us2rmb_rate','hk2rmb_rate')
        if array != 'error' and len(array) == 2:
            us2rmb_rate,hk2rmb_rate = array
        else:
            # hard code
            print('!!! using hard code rate values !!!')
            hk2rmb_rate = 0.8952
            us2rmb_rate = 7.0079

    print("current HKY to RMB rate:{0}\ncurrent dollar to RMB rate:{1}\n".format(hk2rmb_rate,us2rmb_rate))
    
    def process_item(self, item, spider):
        if item['stock_area'] == ' HK':
            item['stock_value'] = float(item['stock_value']) * Foreignmoney2rmyPipeline.hk2rmb_rate
            #item['stock_value'] = '{:.4f}'.format(item['stock_value'])
        elif item['stock_area'] == 'US':
            item['stock_value'] = float(item['stock_value']) * Foreignmoney2rmyPipeline.us2rmb_rate
            #item['stock_value'] = '{:.4f}'.format(item['stock_value'])
        #else:
            #item['stock_value'] = float(item['stock_value']) 
            #item['stock_value'] = int(item['stock_value'] *10000)/10000
        item['stock_value'] = '{:.4f}'.format(float(item['stock_value']))
        return item

# store into mongodb
class MongoDBPipeline(object):
    
    current_date = datetime.datetime.now().strftime('%Y-%m-%d-%p')
    def open_spider(self,spider):
        #Prepare.change_mongo_status(1,0)
        db_uri = spider.settings.get('MONGODB_URI','mongodb://localhost:27017')
        db_name = spider.settings.get('MONGODB_NAME','stockdb')

        
        self.db_client = MongoClient(db_uri)
        self.db = self.db_client[db_name]
        # delete 0 data in stock_latest collection
        self.db.stock_latest.delete_many({"status_flag":0})

    def close_spider(self,spider):
        
        #change status in stock_latest collection
        if self.db.stock_latest.find({"status_flag":0}).count() > 0:
            # delete the data with status_flag=1 in stock_latest
            self.db.stock_latest.delete_many({"status_flag":1})
            Prepare.change_mongo_status(0,1)

        # get time and write it to a time collection
        self.current_date = self.current_date
        Prepare.write_mongo_accessory_collecton(time_stamp=self.current_date)

        self.db_client.close()

    def process_item(self,item,spider):
        self.insert_db(item)
        return item
    
    def insert_db(self,item):
         
        item['time_stamp'] = self.current_date #datetime.datetime.now().strftime('%Y-%m-%d-%p')
        #print( 'cccccc',item['time_stamp'])
        if isinstance(item,Item):
            item = dict(item)
            # stock_value shout not a string
            item["stock_value"] = float(item["stock_value"])
        # then insert into stock collection ,thsi collection contains all the history data 
        #self.db.stock.insert_one(item)
        self.db.stock.update_one({"stock_id": item['stock_id'],"time_stamp":item['time_stamp']},{"$set":{"stock_name": item['stock_name'],"stock_value": item['stock_value'],"stock_id": item['stock_id'],"time_stamp":item['time_stamp'],"stock_area":item['stock_area']}},True)

        # inset into stock_latest collection ,this collection contains the latest stock data
        item['status_flag'] = 0
        self.db.stock_latest.update_one({"stock_id": item['stock_id'],"status_flag":item['status_flag']},{"$set":{"stock_name": item['stock_name'],"stock_value": item['stock_value'],"status_flag":item['status_flag'],"stock_id": item['stock_id'],"time_stamp":item['time_stamp'],"stock_area":item['stock_area']}},True)
        #self.db.stock_latest.insert_one(item)
        
        

