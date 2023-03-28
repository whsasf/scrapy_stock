# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import urllib.request
import urllib.parse
from pymongo import MongoClient
from scrapy import Item
#from scrapy.conf import settings
from scrapy.utils.project import get_project_settings
import datetime
import json
#import random

settings = get_project_settings()
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
        urls = 'http://op.juhe.cn/onebox/exchange/currency?key=594268cfe4f638bcb7f8335a438d06bd&from=SAR&to=CNY' # 沙特里亚尔
        try:
            data_stream = eval(urllib.request.urlopen(url,None,10).read().decode('utf-8').replace('null','"null"'))
            data_stream2 = eval(urllib.request.urlopen(urls,None,10).read().decode('utf-8').replace('null','"null"'))
            if data_stream['resultcode'] == "200":
                print(data_stream['result'][''])
                for data in data_stream['result'][0]:

                    if data_stream['result'][0][data]['name'] == '美元':
                        us2rmb_rate = float(data_stream['result'][0][data]['mSellPri'])/100
                    elif data_stream['result'][0][data]['name'] == '港币':
                        hk2rmb_rate = float(data_stream['result'][0][data]['mSellPri'])/100
            else:
                return ('error')
            
            if data_stream2['reason'] == '查询成功!':
                sar2rmb_rate = float(data_stream2['result'][0]['exchange'])
                print('sar2rmb_rate',sar2rmb_rate)
            else:
                return ('error')
            
        except:
            return ('error')
        if float(us2rmb_rate) >0 and float(hk2rmb_rate) >0 and  sar2rmb_rate >0:
            return (us2rmb_rate,hk2rmb_rate,sar2rmb_rate)
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

    @staticmethod
    def getStockValuePre(mm,sid,sa,svalue):
            #flag1 = datetime.datetime.now().strftime('%p')
            flag1= ''
            if sa.lower() == 'cn' or sa.lower() == 'hk':
                flag1 = 'PM'
            else:
                flag1 = 'AM'
            ii = 1
            stock_value_pre = svalue
            while ii < 30:
                temp_day = str(datetime.date.today() -datetime.timedelta(days=ii))+'-'+flag1
                temp_result = list(mm.find({'stock_id':sid,'time_stamp':temp_day},{"_id":0,'stock_value':1}))
                if temp_result and temp_result[0]['stock_value']:
                    #print(ii)
                    stock_value_pre = temp_result[0]['stock_value']
                    break
                else:
                    ii += 1
            return stock_value_pre


class Foreignmoney2rmyPipeline(object):

    #hk2rmb_rate = 0.8952
    #us2rmb_rate = 7.0079
    #sar2rmb_rate = 1.8703
    oo = Prepare.fetch_rate()
    if oo != 'error' and len(oo) == 3 : #success
        print('fetching rate value successfully!!')
        us2rmb_rate,hk2rmb_rate,sar2rmb_rate = oo
        Prepare.write_mongo_accessory_collecton(us2rmb_rate=us2rmb_rate,hk2rmb_rate=hk2rmb_rate,sar2rmb_rate=sar2rmb_rate)
    else:
        print('reading rate value form database!')
        us2rmb_rate,hk2rmb_rate,sar2rmb_rate = "","",""
        array = Prepare.read_mongo_accessory_collecton('us2rmb_rate','hk2rmb_rate','sar2rmb_rate')
        if array != 'error' and len(array) == 3:
            us2rmb_rate,hk2rmb_rate,sar2rmb_rate = array
        else:
            # hard code
            print('!!! using hard code rate values !!!')
            hk2rmb_rate = 0.8952
            us2rmb_rate = 7.0079
            sar2rmb_rate = 1.8703
        # 也更新时间
        Prepare.write_mongo_accessory_collecton(us2rmb_rate=us2rmb_rate,hk2rmb_rate=hk2rmb_rate,sar2rmb_rate=sar2rmb_rate)

    print("current HKY to RMB rate:{0}\ncurrent dollar to RMB rate:{1}\ncurrent sar to RMB rate:{2}\n".format(hk2rmb_rate,us2rmb_rate,sar2rmb_rate))

    def process_item(self, item, spider):

        if item['stock_area'] == 'HK':
            item['stock_value'] = float(item['stock_value']) * Foreignmoney2rmyPipeline.hk2rmb_rate
            #item['stock_value'] = '{:.4f}'.format(item['stock_value'])
        elif item['stock_area'] == 'US':
            item['stock_value'] = float(item['stock_value']) * Foreignmoney2rmyPipeline.us2rmb_rate
            #item['stock_value'] = '{:.4f}'.format(item['stock_value'])
        elif item['stock_area'].lower() == 'sau':
            item['stock_value'] = float(item['stock_value']) * Foreignmoney2rmyPipeline.sar2rmb_rate
        #else:
            #item['stock_value'] = float(item['stock_value'])
            #item['stock_value'] = int(item['stock_value'] *10000)/10000
        item['stock_value'] = '{:.4f}'.format(float(item['stock_value']))
        return item

# below pipeline settings for get_stock_category -----------------------------------------------------------
# store into mongodb


class Modify_Stock_Name(object):
    """
    this pipeline is used to clear the -sw or -w appendix a stock name
    """
    def process_item(self, item, spider):
        if '-SW' in item['stock_name']:
            item['stock_name'] = item['stock_name'].strip('-SW')
        elif '-W' in item['stock_name']:
            item['stock_name'] = item['stock_name'].strip('-W')
        return item


class MongoDBPipeline(object):

    current_date = datetime.datetime.now().strftime('%Y-%m-%d-%p')
    mucurrent = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M')
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
        # self.current_date = self.current_date
        print("mucurrent", self.mucurrent)
        Prepare.write_mongo_accessory_collecton(time_stamp=self.mucurrent)

        # write latest data into a json file ,for stockhey to read
        #sort_data = list(self.db.stock_latest.find({"status_flag":1,"stock_value":{"$gt":0}},{"stock_id":1,"stock_name":1,"stock_area":1,"stock_value":1,"_id":0}).sort("stock_value",-1))
        raw_data_list = list(self.db.stock_latest.aggregate([{"$lookup":{"from":"stock_info","localField":"stock_id","foreignField":"stock_id","as":"result"}},{"$addFields":{"stock_buss_alias":"$result.stock_buss_alias"}},{"$project":{"_id":0,"stock_id":1,"stock_area":1,"stock_come":1,"stock_percent":1,"stock_name":1,"stock_value":1,"stock_buss_alias":1}}]))
        #print(len(raw_data_list))
        #print(raw_data_list)
        for mm  in raw_data_list:
            try:
                #if mm['stock_buss_alias'][0]:
                mm['stock_buss_alias'] = mm['stock_buss_alias'][0]
                #if raw_data_list[indexxx]['stock_value'] == 0:
                #    raw_data_list.pop(indexxx)
            except IndexError:
                pass
        #sort
        sort_data_list = sorted(raw_data_list, key=lambda x : x['stock_value'], reverse=True)
        # choose CN and US based on stock_come
        sort_data_list_CN = []
        sort_data_list_US = []

        for indexxx  in range(0,len(sort_data_list)):
            if sort_data_list[indexxx].get('stock_come','NA') == 'US':
                sort_data_list_US.append(sort_data_list[indexxx])
            elif sort_data_list[indexxx].get('stock_come','NA') == 'CN':
                sort_data_list_CN.append(sort_data_list[indexxx])

        # add NO for CN
        for indexxx  in range(0,len(sort_data_list_CN)):
            sort_data_list_CN[indexxx]['index'] = indexxx + 1
            sort_data_list_CN[indexxx].pop('stock_come')
            # add stock_stock_pre
            # pre_value = Prepare.getStockValuePre(self.db.stock,sort_data_list_CN[indexxx]['stock_id'],sort_data_list_CN[indexxx]['stock_value'])
            # if pre_value == 0:
            #     pre_value_x = 1
            # else:
            #     pre_value_x = pre_value
            # temp_percent = (sort_data_list_CN[indexxx]['stock_value'] - pre_value )/ pre_value_x
            # sort_data_list_CN[indexxx]['stock_percent'] = float(format(temp_percent * 100,'.4f'))
            #print(sort_data_list_CN[indexxx])
        # add NO for US
        for indexxx  in range(0,len(sort_data_list_US)):
            sort_data_list_US[indexxx]['index'] = indexxx + 1
            sort_data_list_US[indexxx].pop('stock_come')
            # add stock_stock_pre
            # pre_value = Prepare.getStockValuePre(self.db.stock,sort_data_list_US[indexxx]['stock_id'],sort_data_list_US[indexxx]['stock_value'])
            # if pre_value == 0:
            #     pre_value_x = 1
            # else:
            #     pre_value_x = pre_value
            # temp_percent = (sort_data_list_US[indexxx]['stock_value'] - pre_value )/ pre_value_x
            # sort_data_list_US[indexxx]['stock_percent'] = float(format(temp_percent * 100,'.4f'))
            #print(sort_data_list_US[indexxx])
            # get only top 500 for CN
        sort_data_list_CN = sort_data_list_CN[0:501]
        #print (sort_data_list_CN)
        #print (sort_data_list_US)

        # get
        #function to get any date before today
        def getYesterdays(num):
            temp_days = []
            today=datetime.date.today()
            for day_num in range(0,num):
                oneday=datetime.timedelta(days=day_num)
                anyday=today-oneday
                if anyday.isoweekday() != 7:
                    # do not get sunday ,since it's total closed
                    temp_days.append(str(anyday))
            #print(temp_days)
            return temp_days

        def echart():
            """
            this function is ot generate json data that echarts needed in frontend
            """
            #prapare data for viaualization
            # 1 get date list
            date_range = getYesterdays(10) # 10 days by default
            # 2 get stock_name and stock_value in each day
            final = {}
            real_date_range = []
            # all_data = {}
            stock_value_data = {}
            percent_data_up = {}
            percent_data_down = {}
            # all_data_top5 = {}
            # CopList_dict = {}
            for date in date_range:
                temp1 = date+'-PM'
                temp2 = date+'-AM'
                if self.db.stock.count_documents({"time_stamp":temp1}) > 0:
                    raw_data = list(self.db.stock.find({"time_stamp":temp1,"stock_value":{"$gt":0}},{"stock_name":1,"stock_area":1,"stock_value":1,"stock_percent":1, "_id":0}))
                else:
                    raw_data = list(self.db.stock.find({"time_stamp":temp2,"stock_value":{"$gt":0}},{"stock_name":1,"stock_area":1,"stock_value":1,"stock_percent":1, "_id":0}))
                #print(raw_data)
                if raw_data:
                    real_date_range.append(date)
                    value_raw = sorted(raw_data, key=lambda x : x['stock_value'], reverse=True)
                    value1 = value_raw[0:30] # get top 30
                    try:
                        percent_up_raw = sorted(raw_data, key=lambda x : x['stock_percent'], reverse=False)
                        percent_up = percent_up_raw[-30:]
                    except:
                        percent_up = []
                    try:
                        percent_down_raw = sorted(raw_data, key=lambda x : x['stock_percent'], reverse=True)
                        percent_down = percent_down_raw[-30:]
                    except:
                        percent_down = []

                    #out_a = value_raw[0:5] # get top 5
                    #random.shuffle(out) # break the order
                    value2 = [{"name":x['stock_name']+'-'+x['stock_area'],"value":x['stock_value']} for x in value1]
                    percent_up2 = [{"name":x['stock_name']+'-'+x['stock_area'],"value":x['stock_percent']} for x in percent_up]
                    percent_down2 = [{"name":x['stock_name']+'-'+x['stock_area'],"value":x['stock_percent']} for x in percent_down]
                    #out_a2 = [{"name":x['stock_name']+'-'+x['stock_area'],"value":x['stock_value']} for x in out_a]
                    #print(out2)
                    stock_value_data[date] = value2
                    percent_data_up[date] = percent_up2
                    percent_data_down[date] = percent_down2
                    # all_data_top5[date] = out_a2
                    # CopList_dict[date] = [x['name'] for x in out2]
            final['real_date_range'] = real_date_range
            final['stock_value_data'] = stock_value_data
            final['percent_data_up'] = percent_data_up
            final['percent_data_down'] = percent_data_down
            # final['all_data_top5'] = all_data_top5
            # final['CopList_dict'] = CopList_dict
            #print(final)
            return (final)

        allrank = echart()

        # wirite json to json file
        # CN
        with open('/data/whsasf/stockhey_project/static/file/rankdatacn.json','w',encoding='utf-8')  as filehander:
            json.dump(sort_data_list_CN,filehander,ensure_ascii=False,indent=4)
        #US
        with open('/data/whsasf/stockhey_project/static/file/rankdataus.json','w',encoding='utf-8')  as filehander:
            json.dump(sort_data_list_US,filehander,ensure_ascii=False,indent=4)
        # allrank
        with open('/data/whsasf/stockhey_project/static/file/allrank.json','w',encoding='utf-8')  as filehander:
            json.dump(allrank,filehander,ensure_ascii=False,indent=4)
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
            # add stock_percent
            pre_value = Prepare.getStockValuePre(self.db.stock,item['stock_id'],item['stock_area'],item['stock_value'])
            if pre_value == 0:
                if item['stock_value'] != 0:
                    pre_value = item['stock_value']
                    pre_value_x = 1
            else:
                pre_value_x = pre_value
            temp_percent = (item['stock_value'] - pre_value )/ pre_value_x
            item['stock_percent'] = float(format(temp_percent * 100,'.4f'))
        # then insert into stock collection ,thsi collection contains all the history data
        #self.db.stock.insert_one(item)
        self.db.stock.update_one({"stock_id": item['stock_id'],"time_stamp":item['time_stamp']},{"$set":{"stock_name": item['stock_name'],"stock_value": item['stock_value'],"stock_id": item['stock_id'],"time_stamp":item['time_stamp'],"stock_area":item['stock_area'],"stock_percent":item['stock_percent'],"stock_come":item['stock_come']}},True)

        # inset into stock_latest collection ,this collection contains the latest stock data
        item['status_flag'] = 0
        self.db.stock_latest.update_one({"stock_id": item['stock_id'],"status_flag":item['status_flag']},{"$set":{"stock_name": item['stock_name'],"stock_percent":item['stock_percent'],"stock_value": item['stock_value'],"status_flag":item['status_flag'],"stock_id": item['stock_id'],"time_stamp":item['time_stamp'],"stock_area":item['stock_area'],"stock_come":item['stock_come']}},True)
        #self.db.stock_latest.insert_one(item)
#-----------------------------------------------------------------------------------------------------------


# below pipeline settings for get_stock_category -----------------------------------------------------------

class MongoDBPipeline2(object):

    def open_spider(self,spider):
        db_uri = spider.settings.get('MONGODB_URI','mongodb://localhost:27017')
        db_name = spider.settings.get('MONGODB_NAME','stockdb')

        self.db_client = MongoClient(db_uri)
        self.db = self.db_client[db_name]

    def close_spider(self,spider):
        self.db_client.close()

    def process_item(self,item,spider):
        self.insert_db(item)
        return item

    def insert_db(self,item):
        if isinstance(item,Item):
            item = dict(item)
        stock_id = item['stock_id']
        stock_id_buss_alias_already_list = list(self.db.stock_info.find({"stock_id":stock_id},{"_id":0,"stock_buss_alias":1}))
        #print(stock_id_buss_alias_already_list)
        if len(stock_id_buss_alias_already_list) > 0:
            stock_id_buss_alias_already = stock_id_buss_alias_already_list[0]['stock_buss_alias']
            #print(stock_id_buss_alias_already)
            if stock_id_buss_alias_already == 'NULL' and item['stock_buss_alias']:
                self.db.stock_info.update_one({"stock_id": item['stock_id']},{"$set":{"stock_id": item.get('stock_id','NULL'),"stock_name": item.get('stock_name','NULL'),"stock_buss_alias": item.get('stock_buss_alias','NULL'),"stock_buss_official": item.get('stock_buss_official','NULL'),"stock_area":item.get('stock_area','NULL')}},True)
        else:
            self.db.stock_info.update_one({"stock_id": item['stock_id']},{"$set":{"stock_id": item.get('stock_id','NULL'),"stock_name": item.get('stock_name','NULL'),"stock_buss_alias": item.get('stock_buss_alias','NULL'),"stock_buss_official": item.get('stock_buss_official','NULL'),"stock_area":item.get('stock_area','NULL')}},True)
