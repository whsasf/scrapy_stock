# -*- coding: utf-8 -*-
import scrapy
import sys
sys.path.append('/Library/Python/3.7/site-packages')
from scrapy_splash import SplashRequest
from ..items import StockItem
import re
import datetime

long_loading_wait_time = 3
middle_loading_wait_time = 1
short_loading_wait_time = 0.2
rendering_page_timeout = 60

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


#total_page = 0
#regex_space = re.compile('\s+')
#regex_stock_id = re.compile('(\d+)')
regex_stock_info = re.compile('(.*?\(*.*\)*)(\s)*\((.*)\)')
#regex_stock_value = re.compile('(.*)亿')
#regex_stock_value_wan = re.compile('(.*)万亿')
regex_stock_value = re.compile('(\d+\.*\d+)')

class StockSpider(scrapy.Spider):
    name = 'stock'
    allowed_domains = ['eastmoney']
    start_urls = [
        'http://quote.eastmoney.com/center/gridlist.html#hk_wellknown',
        'http://quote.eastmoney.com/center/gridlist.html#us_chinese'
        ]
    #total_page = 0

    def extract_page(self,response):
        """
        this function is used to extarctc the expected data from page
        """
        #raw_title = response.xpath("substring-before(//head/title,')')").extract()[0]
        raw_title = response.xpath('//head/title/text()').extract()[0]
        #print(raw_title)
        #(stock_name,stock_id) = regex_space.split(raw_title)
        #stock_id = re.findall(regex_stock_id, stock_id)[0]
        (stock_name,_,stock_id) = re.findall(regex_stock_info,raw_title)[0]
        stock_area = response.meta['stock_area']
        stock_value = response.xpath('//li[contains(text(),"总市值")]/i/text()').extract()[0]
        
        if '万亿' in stock_value:
            stock_value = float(re.findall(regex_stock_value,stock_value)[0])*10000
        elif '万' in  stock_value and not '亿' in  stock_value:
            stock_value = float(re.findall(regex_stock_value,stock_value)[0])/10000
        else:
            stock_value = re.findall(regex_stock_value,stock_value)[0]
        #print(stock_name,stock_area,stock_id,stock_value)
        
        stock = StockItem()
        stock["stock_name"]=stock_name.strip(),
        stock["stock_id"]=stock_id.strip(),
        stock["stock_area"]=stock_area.strip(),
        stock["stock_value"]=stock_value.strip()

        yield stock
        

    def parse(self, response):
       url_list = response.xpath('//td[@class="mywidth"]/a/@href').extract()
       stock_area = response.meta['stock_area']
       for url in url_list:
           #real_url = http:///quote.eastmoney.com/unify/r/116.03309 ,# for example
           real_url = 'http:'+ url
           #print(real_url)
           yield SplashRequest(real_url,endpoint = 'execute',args = {'lua_source':lua_extract_page ,'images': 0,'timeout': rendering_page_timeout},callback=self.extract_page,dont_filter=True,meta={'stock_area':stock_area})


    def parse_page_num(self, response):
        stock_area = response.meta['stock_area']
        url = response.url
        #print('url='+url)
        total_page = int(response.xpath('//span[@class="paginate_page"]/a[contains(@class,"paginate_button") and not (contains(@class,"disabled"))][last()]/text()').extract_first())
        #print("total_page="+str(total_page))
        #yield {"total_page":total_page}
        if total_page >1:
            for page in range(1,total_page+1):
                real_lua_fetch_pages = lua_fetch_pages.format(page)
                #print(page,real_lua_fetch_pages)
                yield SplashRequest(url,endpoint = 'execute',args = {'lua_source':real_lua_fetch_pages ,'images': 0,'timeout': rendering_page_timeout},callback=self.parse,dont_filter=True,meta={'stock_area':stock_area})
            

    def start_requests(self):

        for url in self.start_urls:
            # identify the stock area ,like HK, US ,etc
            if 'hk' in url.lower():
               stock_area = 'HK'
            elif 'us' in  url.lower():
               stock_area = 'US'

            yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': lua_script_getfirst_page ,'images': 0,'timeout': rendering_page_timeout},callback=self.parse_page_num,meta={'stock_area':stock_area})

