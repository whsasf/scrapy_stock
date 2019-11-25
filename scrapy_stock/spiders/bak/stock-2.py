# -*- coding: utf-8 -*-
import scrapy
import sys

class StockSpider(scrapy.Spider):

    name = 'stock'
    allowed_domains = ['eastmoney']
    each_page_item_num = 100   #items number in each page
    total_num = 100000000000 # initial total number of items
    start_urls = [
        #HK
        'http://25.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112409399375505555292_1572775248912&pn={0}&pz={1}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:MK0106&fields=f2,f12,f14,f20'
        ]

    def start_requests(self):
        for url in self.start_urls:
            i = 1
            while True:
                real_url = url.format(i,each_page_item_num)
                if i * each_page_item_num <= total_num:
                    i = i+1
                    yield scrapy.Request(real_url,callback = self.parse)
                else:
                    break

    def parse(self, response):
        print(response.body)

