# -*- coding: utf-8 -*-
import scrapy
import sys
sys.path.append('/Library/Python/3.7/site-packages')
from scrapy_splash import SplashRequest
from ..items import StockItem
import re
import datetime

class StockSpider(scrapy.Spider):
    name = 'stock'
    #allowed_domains = ['eastmoney']
    start_urls = {
        'us_chinese':'http://quote.eastmoney.com/center/gridlist.html#us_chinese',
        'hs_a_board':'http://quote.eastmoney.com/center/gridlist.html#hs_a_board',
        'hk_wellknown':'http://quote.eastmoney.com/center/gridlist.html#hk_wellknown',
        'hk_bluechips':'http://quote.eastmoney.com/center/gridlist.html#hk_bluechips',
        'hk_redchips':'http://quote.eastmoney.com/center/gridlist.html#hk_redchips',
        'hk_components':'http://quote.eastmoney.com/center/gridlist.html#hk_components',   
    }
    #total_page = 0

    long_loading_wait_time = 3
    middle_loading_wait_time = 1
    short_loading_wait_time = 0.6
    rendering_page_timeout = 60
    hsa_default_pages = 20

    #get first page ,use it to get pagenum
    lua_script_getfirst_page = """
    function main(splash, args)
      assert(splash:go(args.url))
      assert(splash:wait({0}))
       return {{
        html = splash:html()
      }}
    end        
    """.format(long_loading_wait_time)

    #reauest each page，in a loop, input page number from 1 to page_num,then click go
    lua_fetch_pages = """
    function main(splash, args)
        assert(splash:go(args.url))
        assert(splash:wait({0}))
        js1 = string.format('document.querySelector(".paginate_input").value={{0}}', args.page)
        js2 = string.format('document.querySelector(".paginte_go").click();', args.page)
        splash:runjs(js1)
        assert(splash:wait({1}))
        splash:runjs(js2)
        assert(splash:wait({2}))
        return {{{{
            html = splash:html()
        }}}}
    end
    """.format(long_loading_wait_time,short_loading_wait_time,long_loading_wait_time)

    #extract data from  page
    lua_extract_page = """
    function main(splash, args)
      assert(splash:go(args.url))
      assert(splash:wait({0}))
       return {{
        html = splash:html()
      }}
    end        
    """.format(long_loading_wait_time)


    # feal with mainland stock pages,only fetch the first 10 pages since it's already sorted desc
    lua_HSA_pages = """
    function main(splash, args)
        assert(splash:go(args.url))
        assert(splash:wait({0}))
        local btn_select_free = splash:select('#custom-fields')
        btn_select_free:mouse_click()
        assert(splash:wait({1}))
        splash:send_text("总市值")
        splash:send_keys("<Enter>")
        assert(splash:wait({2}))
        local btn_sort = splash:select('[aria-label="总市值"]')
      	btn_sort:mouse_click()
        assert(splash:wait({3}))
        local btn_sort_desc = splash:select('[aria-label="总市值"]')
        btn_sort_desc:mouse_click()
        assert(splash:wait({4}))

        js1 = string.format('document.querySelector(".paginate_input").value={{0}}', args.page)
        js2 = string.format('document.querySelector(".paginte_go").click();', args.page)
        splash:runjs(js1)
        assert(splash:wait({5}))
        splash:runjs(js2)
        assert(splash:wait({6}))

        return {{{{
            html = splash:html()
        }}}}
    end
    """.format(long_loading_wait_time,short_loading_wait_time,short_loading_wait_time,short_loading_wait_time,middle_loading_wait_time,short_loading_wait_time,middle_loading_wait_time)

    #regular expression
    regex_stock_value = re.compile('(\d+\.*\d+)')

    def check_stock_value(self,stock_value):
        """
        this function is used to check the stock_value
        """
        #print("stock_value=",stock_value)
        if '万亿' in stock_value:
            stock_value = float(re.findall(self.regex_stock_value,stock_value)[0])*10000
        elif '万' in  stock_value and not '亿' in  stock_value:
            stock_value = float(re.findall(self.regex_stock_value,stock_value)[0])/10000
        elif '亿' in  stock_value and not '万' in  stock_value:
            stock_value = re.findall(self.regex_stock_value,stock_value)[0]
        else:
            stock_value = 0
        return stock_value

    def extract_page(self,response):
        """
        this function is used to extarctc the expected data from page
        """
        stock_area = response.meta['stock_area']
        stock_name = response.meta['stock_name']
        stock_id = response.meta['stock_id']
        stock_value = response.xpath('//li[contains(text(),"总市值")]/i/text()').extract()[0]
        
        stock_value = self.check_stock_value(stock_value)
        #print(stock_name,stock_area,stock_id,stock_value)
        
        stock = StockItem()
        stock["stock_name"]=stock_name
        stock["stock_id"]=stock_id
        stock["stock_area"]=stock_area
        stock["stock_value"]=stock_value

        yield stock
        
    def parse(self, response):
        stock_area = response.meta['stock_area']
        
        url_list = response.xpath('//td[@class="mywidth"]/a/@href').extract()
        name_list = response.xpath('//td[@class="mywidth"]/a/text()').extract()
        stock_id_list = [url.split('.')[-1] for url in url_list]
        #print("name_list",name_list)

        if stock_area == 'HK':
            group_list = list(zip(url_list,name_list,stock_id_list))
            for group in group_list:
                real_url = 'http:'+ group[0]
                stock_name = group[1]
                stock_id = group[2]
                #print(group)
                yield SplashRequest(real_url,endpoint = 'execute',args = {'lua_source':self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.extract_page,dont_filter=False,meta={'stock_name':stock_name,'stock_id':stock_id,'stock_area':stock_area})
        elif stock_area == 'US' or stock_area == 'CN':
            stock_value_list = response.xpath('//*[@id="table_wrapper-table"]/tbody/tr[*]/td[last()-1]/text()').extract()
            stock_value_list = [self.check_stock_value(stock_value) for stock_value in stock_value_list]
            #print("stock_value_list",stock_value_list)
            group_list = list(zip(name_list,stock_id_list,stock_value_list))
            for group in group_list:
                #print(group)
                stock = StockItem()
                stock["stock_name"]=group[0]
                stock["stock_id"]=group[1]
                stock["stock_area"]=stock_area
                stock["stock_value"]=group[2]
                yield stock


    def parse_page_num(self, response):
        stock_area = response.meta['stock_area']
        url = response.url
        #print('url='+url)
        total_page = int(response.xpath('//span[@class="paginate_page"]/a[contains(@class,"paginate_button") and not (contains(@class,"disabled"))][last()]/text()').extract_first())
        #print("total_page="+str(total_page))
        if total_page >0:
            for page in range(1,total_page+1):
                real_lua_fetch_pages = self.lua_fetch_pages.format(page)
                #print(page,real_lua_fetch_pages)
                yield SplashRequest(url,endpoint = 'execute',args = {'lua_source':real_lua_fetch_pages ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse,dont_filter=True,meta={'stock_area':stock_area})
    

    def start_requests(self):
        for title in self.start_urls:
            url = self.start_urls[title]
            # identify the stock area ,like HK, US ,etc
            if 'hk' in title.lower():
               stock_area = 'HK'
               yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': self.lua_script_getfirst_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse_page_num,meta={'stock_area':stock_area})
            elif 'us' in  title.lower():
               stock_area = 'US'
               yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': self.lua_script_getfirst_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse_page_num,meta={'stock_area':stock_area})
            elif 'hs_a' in  title.lower():
               stock_area = 'CN'
               for i in range(1,self.hsa_default_pages+1):
                   real_lua_source = self.lua_HSA_pages.format(i)
                   #print(real_lua_source)
                   yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': real_lua_source ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse,dont_filter=True,meta={'stock_area':stock_area})
