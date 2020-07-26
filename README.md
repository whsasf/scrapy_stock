# 股票信息爬取系统 #


## 1. 介绍

### 1. 本scrapy爬虫爬取的是"东方财富网数据"   
### 2. 爬取内容为: 知名美股 + 前400沪深A股 + 部分港股
### 3. 使用splash作为动态信息解析引擎，具体安装请参考 [splash](https://splash.readthedocs.io/en/stable/#)
### 4. 数据库使用mongodb

## 2. 使用方式
### 1. 克隆项目到本地（最好到虚拟环境中）: git clone https://github.com/whsasf/scrapy_stock.git
### 2. 安装项目依赖: pip install -r requirements.txt
### 3. 爬取股票市值信息: scrapy crawl stocck
### 4. 爬取股票类别信息: scrapy crawl get_stock_category

