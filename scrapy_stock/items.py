# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class ScrapyStockItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class StockItem(Item):
    stock_name = Field()
    stock_id = Field()
    stock_value = Field()
    stock_come = Field()
    stock_area = Field()
    #status_flag :1 = latest, 0 = middle state 
    #status_flag = Field() # unly used in stock_latest collection
    time_stamp = Field()

class Stock_bussItem(Item):
    stock_name = Field()
    stock_id = Field()
    stock_area = Field()
    stock_buss_alias = Field() # easy to understand, used as default
    stock_buss_official = Field() # office from zhengjianhui
    


