# -*- coding: utf-8 -*-
import scrapy
import sys
sys.path.append('/Library/Python/3.7/site-packages')
from scrapy_splash import SplashRequest
from ..items import StockItem

# 获取第一个页面
lua_fetch_firstpage = """
function main(splash, args)
    assert(splash:go(args.url))
    assert(splash:wait(0.5))
    return {
        html = splash:html()
    }
end
"""

#获取第二到最后一页的页面，每一次都需要输入页码数，并点击'go'
lua_fetch_otherpages = """
function main(splash, args)
    assert(splash:go(args.url))
    assert(splash:wait(0.5))
    js1 = string.format('document.querySelector(".paginate_input").value={0};', args.page)
    js2 = string.format('document.querySelector(".paginte_go").click();', args.page)
    splash:runjs(js1)
    assert(splash:wait(0.1))
    splash:runjs(js2)
    assert(splash:wait(0.5))
    return {{
        html = splash:html()
    }}
end
"""


class StockSpider(scrapy.Spider):
    name = 'stock'
    allowed_domains = ['eastmoney']
    #allowed_domains = ['jenkins.io']
    start_urls = ['http://quote.eastmoney.com/center/gridlist.html#hk_wellknown']
    #start_urls = ['https://jenkins.io/zh/doc/pipeline/tour/agents/']
    init_flag = 1

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': lua_fetch_firstpage ,'images': 0,'timeout': 10})
    

    def parse(self, response):
        #stock = StockItem()
        for link in response.xpath("/html/body/div[@class='page-wrapper']/div[@id='page-body']/div[@id='body-main']/div[@id='table_wrapper']/div[@class='listview full']/table[@id='table_wrapper-table']/tbody/tr[*][*]/td[@class='mywidth']/a/text()").extract():
            #yield 
            print(link)
        
        #if init_flag == 1:
        # 获取总页数 的 xpath  //*[@id="main-table_paginate"]/span[1]/a/text() ，然后取最后一个
        total_page_list = response.xpath('//*[@id="main-table_paginate"]/span[1]/a/text()').extract()
        total_page = int(total_page_list[len(total_page_list)-1])
        #print('total_page:'+str(total_page))
        #init_flag = init_flag + 1
        # 循环剩余的页数： 2 : total_page
        for page_num in range(2,total_page+1):
            for url in self.start_urls:
                yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': lua_fetch_otherpages.format(page_num) ,'images': 0,'timeout': 10})


    #def extract_final_page():
