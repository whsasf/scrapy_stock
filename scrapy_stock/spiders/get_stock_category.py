# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
from ..items import Stock_bussItem
import cfscrape
from lxml import html

class GetStockCategorySpider(scrapy.Spider):
    name = 'get_stock_category'
    #allowed_domains = ['eastmoney.com']
    #start_urls = ['http://eastmoney.com/']

    # use specific ITEM_PIPELINES for thsi spider
    custom_settings = {
        "DOWNLOAD_DELAY" : 0.5,
        'ITEM_PIPELINES':{
        #'scrapy_stock.pipelines.Modify_Stock_Name': 300,
        'scrapy_stock.pipelines.MongoDBPipeline2': 400
        }
    }

    start_urls = {
        'sp_sau': 'https://cn.investing.com/equities/saudi-aramco', #pass
        
        #'sp_hk':'https://emweb.securities.eastmoney.com/PC_HKF10/CoreReading/index?type=web&code=09988&color=b', # 已经被覆盖
	    #'sp2_hk':'https://emweb.securities.eastmoney.com/PC_HKF10/CoreReading/index?type=web&code=09999&color=b', # 已经被覆盖
        
        'united_states':'https://quote.eastmoney.com/center/gridlist.html#us_stocks', #pass
        'us_chinese':'https://quote.eastmoney.com/center/gridlist.html#us_chinese', # pass
        'hs_a_board':'https://quote.eastmoney.com/center/gridlist.html#hs_a_board', # pass 需要splash.private_mode_enabled = false，因为该网页需要使用localstorage
        'hk_wellknown':'https://quote.eastmoney.com/center/gridlist.html#hk_wellknown', # pass
        'hk_bluechips':'https://quote.eastmoney.com/center/gridlist.html#hk_bluechips', # pass
        'hk_redchips':'https://quote.eastmoney.com/center/gridlist.html#hk_redchips', #pass
        'hk_components':'https://quote.eastmoney.com/center/gridlist.html#hk_components', #pass
    }


    long_loading_wait_time = 3
    middle_loading_wait_time = 1
    short_loading_wait_time = 0.6
    rendering_page_timeout = 70
    hsa_default_pages =  20 #20
    united_states_pages = 20


    #extract data from  page
    lua_extract_page = """
    function main(splash, args)
      splash.private_mode_enabled = false
      assert(splash:go(args.url))
      assert(splash:wait({0}))
       return {{
        html = splash:html()
      }}
    end
    """.format(long_loading_wait_time)


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


    # deal with mainland stock pages,only fetch the first 10 pages since it's already sorted desc
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

    # united_states top 200
    lua_United_states_pages = """
    function main(splash, args)
        assert(splash:go(args.url))
        assert(splash:wait({0}))
        local btn_sort = splash:select('[aria-label="总市值(美元)"]')
      	btn_sort:mouse_click()
        assert(splash:wait({1}))
        local btn_sort_desc = splash:select('[aria-label="总市值(美元)"]')
        btn_sort_desc:mouse_click()
        assert(splash:wait({2}))
        js1 = string.format('document.querySelector(".paginate_input").value={{0}}', args.page)
        js2 = string.format('document.querySelector(".paginte_go").click();', args.page)
        splash:runjs(js1)
        assert(splash:wait({3}))
        splash:runjs(js2)
        assert(splash:wait({4}))

        return {{{{
            html = splash:html()
        }}}}
    end
    """.format(long_loading_wait_time,short_loading_wait_time,middle_loading_wait_time,short_loading_wait_time,middle_loading_wait_time)


    #def parse(self, response):
    #    pass

    def extract_page(self,response):
        """
        this function is used to extarctc the expected data from page
        """
        stock_area = response.meta['stock_area']
        stock_id = response.meta['stock_id']
        stock_name = response.meta['stock_name']

        if stock_area.lower() == 'sau':
            stock_buss_alias = response.meta['stock_buss_alias']
            stock_buss_official = stock_buss_alias
        if stock_area == 'HK':
            stock_buss_official = 'NULL'
            if  stock_id == 'NN' and stock_name == 'NN':
                try:
                    stock_buss_alias = response.xpath('//*[starts-with(@id,"rnd_")]/table/tbody/tr[6]/td[4]/text()').extract()[-1].replace('\n','').replace('\r','').strip(' ')
                except:
                    stock_buss_alias = 'NULL'
                stock_id = response.xpath('//*[@id="title"]/div/h1/span[1]/text()').extract()[0].split('.')[0]
                stock_name = response.xpath('//*[@id="title"]/div/h1/span[2]/text()').extract()[0].strip(' ')
            else:
                try:
                    stock_buss_alias = response.xpath('//*[starts-with(@id,"rnd_")]/table/tbody/tr[6]/td[4]/text()').extract()[-1].replace('\n','').replace('\r','').strip(' ')
                except:
                    stock_buss_alias = 'NULL'

        elif stock_area == 'CN':
            try:
                stock_buss_alias = response.xpath('//*[@id="Table0"]/tbody/tr[7]/td[2]/text()').extract()[0].replace('\n','').replace('\r','').strip(' ')
            except:
                stock_buss_alias = 'NULL'
            try:
                stock_buss_official = response.xpath('//*[@id="Table0"]/tbody/tr[8]/td[2]/text()').extract()[0].replace('\n','').replace('\r','').strip(' ')
            except:
                stock_buss_official = 'NULL'
        elif stock_area == 'US':
            stock_buss_official = 'NULL'
            try:
                stock_buss_alias = response.xpath('//*[@id="app"]/div[4]/div/div[2]/div[2]/table/tbody/tr[3]/td[1]/text()').extract()[0].replace('\n','').replace('\r','').strip(' ')
            except:
                stock_buss_alias = 'NULL'

        stock_busses = Stock_bussItem()
        stock_busses["stock_name"]=stock_name.strip(' ')
        stock_busses["stock_id"]=stock_id.strip(' ')
        stock_busses["stock_area"]=stock_area.strip(' ')
        #if stock_buss_alias:
        stock_busses["stock_buss_alias"]=stock_buss_alias.strip(' ')
        #if stock_buss_official:
        stock_busses["stock_buss_official"]=stock_buss_official.strip(' ')

        #print(dict(stock_busses))
        yield stock_busses

    def parse_info(self, response):
        stock_area = response.meta['stock_area']
        stock_id = response.meta['stock_id']
        stock_name = response.meta['stock_name']
        info_url = ""
        if stock_area == 'CN':
            info_url = response.xpath('//*[@id="app"]/div/div/div[*]/div[2]/div[1]/a[2]/@href').extract()[-1].replace("http://", "https://")
            if not info_url.startswith("https://"):
                info_url = "https:" + info_url
        elif stock_area == 'US':
            info_url = response.xpath('//*[@id="us_f10"]/span[3]/a/@href').extract()[0].replace("http://", "https://")
            if not info_url.startswith("https://"):
                info_url = "https:" + info_url
        yield SplashRequest(info_url,endpoint = 'execute',args = {'lua_source':self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.extract_page,dont_filter=False,meta={'stock_area':stock_area,"stock_name":stock_name,"stock_id":stock_id})


    def parse(self, response):
        name_list = ""
        stock_area = response.meta['stock_area']
        url_list = response.xpath('//td[@class="mywidth"]/a/@href').extract()
        stock_id_list = [url.split('.')[-1] for url in url_list]

        if stock_area == 'HK':
            name_list = response.xpath('//*[@id="table_wrapper-table"]/tbody/tr[*]/td[3]/a/descendant-or-self::*/text()').extract()
            info_url_list = response.xpath('//*[@id="table_wrapper-table"]/tbody/tr[*]/td[4]/a[3]/@href').extract()
            group_list = list(zip(info_url_list,name_list,stock_id_list))
            for group in group_list:
                url = group[0].replace("http://", "https://")
                if not url.startswith("https://"):
                    url = "https:" + url
                stock_name = group[1]
                stock_id = group[2]
                yield SplashRequest(url,endpoint = 'execute',args = {'lua_source':self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.extract_page,dont_filter=False,meta={'stock_area':stock_area,"stock_name":stock_name,"stock_id":stock_id})
        elif stock_area == 'CN' or stock_area == 'US':
            if stock_area == 'US':
                name_list = response.xpath('//*[@id="table_wrapper-table"]/tbody/tr[*]/td[2]/a/descendant-or-self::*/text()').extract()
            else:
                name_list = response.xpath('//*[@id="table_wrapper-table"]/tbody/tr[*]/td[3]/a/descendant-or-self::*/text()').extract()
            group_list = list(zip(url_list,name_list,stock_id_list))
            for group in group_list:
                url = group[0].replace("http://", "https://")
                if not url.startswith("https://"):
                    url = "https:" + url
                stock_name = group[1]
                stock_id = group[2]
                yield SplashRequest(url,endpoint = 'execute',args = {'lua_source':self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse_info,dont_filter=True,meta={'stock_area':stock_area,"stock_name":stock_name,"stock_id":stock_id})


    # get totalpage num
    def parse_page_num(self, response):
            stock_area = response.meta['stock_area']
            url = response.url
            total_page = int(response.xpath('//span[@class="paginate_page"]/a[contains(@class,"paginate_button") and not (contains(@class,"disabled"))][last()]/text()').extract_first())
            if total_page >0:
                for page in range(1,total_page+1):
                    real_lua_fetch_pages = self.lua_fetch_pages.format(page)
                    yield SplashRequest(url,endpoint = 'execute',args = {'lua_source':real_lua_fetch_pages ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse,dont_filter=True,meta={'stock_area':stock_area})




    def start_requests(self):
        for title in self.start_urls:
            url = self.start_urls[title]
            # identify the stock area ,like HK, US ,etc

            if 'sau' in title.lower():
                stock_area = 'SAU'
                scraper = cfscrape.create_scraper(delay=30)
                responce = scraper.get(url)
                print("responce", responce)
                response = html.fromstring(responce.text)
                stock_name = ""
                stock_id = 0
                stock_name_id = response.xpath('(//h1)[1]/text()')
                stock_buss_alias = response.xpath('//*[@id="__next"]/div[2]/div[2]/div/div[1]/div/div[10]/div/div[2]/div[1]/a/text()')
                if len(stock_name_id) > 0:
                    stock_name_id = stock_name_id[0]
                    tt = stock_name_id.split(' ')
                    if len(tt) > 1:
                        stock_name = stock_name_id.split(' ')[0]
                        stock_id = stock_name_id.split(' ')[1][1:-1]
                if len(stock_buss_alias) > 0 :
                    stock_buss_alias = stock_buss_alias[0]
                else:
                    stock_buss_alias = 'NULL'
                yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.extract_page,meta={'stock_area':stock_area,'stock_id':stock_id,'stock_name':stock_name, 'stock_buss_alias' : stock_buss_alias })
            elif 'hk' in title.lower():
                stock_area = 'HK'
                if not 'sp' in  title.lower():
                    yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse_page_num,dont_filter=True,meta={'stock_area':stock_area})
                else:
                    # direct extract page
                    yield SplashRequest(url,endpoint = 'execute',args = {'lua_source':self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.extract_page,dont_filter=False,meta={'stock_area':stock_area,"stock_id":'NN','stock_name':"NN"})

            elif 'us_chinese' in  title.lower():
                stock_area = 'US'
                yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': self.lua_extract_page ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse_page_num,dont_filter=True,meta={'stock_area':stock_area})
            elif 'united_states' in  title.lower():
                stock_area = 'US'
                for i in range(1,self.united_states_pages+1):
                    real_lua_source = self.lua_United_states_pages.format(i)
                    yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': real_lua_source ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse,dont_filter=True,meta={'stock_area':stock_area})
            elif 'hs_a_board' in  title.lower():
                stock_area = 'CN'
                for i in range(1,self.hsa_default_pages+1):
                    real_lua_source = self.lua_HSA_pages.format(i)
                    yield SplashRequest(url,endpoint = 'execute',args = {'lua_source': real_lua_source ,'images': 0,'timeout': self.rendering_page_timeout},callback=self.parse,dont_filter=True,meta={'stock_area':stock_area})