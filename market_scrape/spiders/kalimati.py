import scrapy
import csv
from bikram import convert_ad_to_bs
from datetime import date

from pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from datetime import datetime
from scrapy.spiders import CrawlSpider

MONTHS = {'January': '01',
          'February': '02',
          'March': '03',
          'April': '04',
          'May': '05',
          'June': '06',
          'July': '07',
          'August': '08',
          'September': '09',
          'October': '10',
          'November': '11',
          'December': '12'}


class KalimatiSpider(scrapy.Spider):
    name = "kalimati"
    start_urls = ['http://kalimatimarket.gov.np/home/language/EN']
    commodity_wise_price_url = 'http://kalimatimarket.gov.np/commodity-wise-price-information'
    price_type = ['W', 'R']
    product_price = {}

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def parse(self, response):
        yield scrapy.Request(url=self.commodity_wise_price_url, callback=self.parse_data)

    def parse_data(self, response):
        html_response = Selector(text=response.body_as_unicode())
        id_list = html_response.css('select.commodityid option::attr(value)').getall()
        product_list = html_response.css('select.commodityid option::text').getall()
        for product_id, product_name in zip(id_list, product_list):
            self.product_price[product_name] = {}
            for current_price_type in self.price_type:
                data = {'commodityid': product_id,
                        'fromdate': '04/13/1944',  # equivalent to 2001-01-01 B.S.
                        'todate': datetime.strptime(str(date.today()), '%Y-%m-%d').strftime('%m/%d/%Y'),
                        'pricetype': current_price_type}
                yield scrapy.FormRequest(url='http://kalimatimarket.gov.np/priceinfo/commoditypriceview',
                                         formdata=data, callback=self.parse_market_price,
                                         meta={'product_name': product_name, 'current_price_type': current_price_type})

    def parse_market_price(self, response):
        product_name = response.meta.get('product_name')
        current_price_type = response.meta.get('current_price_type')
        html_response = Selector(text=response.body_as_unicode())
        data = html_response.css('center table table tr').getall()[2:]
        if data:
            for row in data:
                row_html = Selector(text=row)
                data = row_html.css('td::text').getall()
                split_date = data[0].split(' ')
                month = MONTHS[split_date[0]]
                year_date_list = split_date[1].split('-')[::-1]
                day = year_date_list[1].zfill(2)
                eng_date = date(int(year_date_list[0]), int(month), int(day))
                minimum = data[1]
                maximum = data[2]
                nep_date = str(convert_ad_to_bs(eng_date))

                if self.product_price[product_name].get(str(eng_date), None):
                    product_price_date_dict = self.product_price[product_name][str(eng_date)]
                    if current_price_type == 'W':
                        product_price_date_dict['wholesale_min'] = minimum
                        product_price_date_dict['wholesale_max'] = maximum
                    elif current_price_type == 'R':
                        product_price_date_dict['retail_min'] = minimum
                        product_price_date_dict['retail_max'] = maximum
                else:
                    if current_price_type == 'W':
                        self.product_price[product_name][str(eng_date)] = {'eng_date': eng_date,
                                                                           'nep_date': nep_date,
                                                                           'wholesale_min': minimum,
                                                                           'wholesale_max': maximum}
                    elif current_price_type == 'R':
                        self.product_price[product_name][str(eng_date)] = {'eng_date': eng_date,
                                                                           'nep_date': nep_date,
                                                                           'retail_min': minimum,
                                                                           'retail_max': maximum}

    def spider_closed(self, spider):
        for name, date_dict in self.product_price.items():
            if date_dict:
                with open(f'kalimati_{name}.csv', 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(["eng_date", "nep_date", "wholesale_min", "wholesale_max", "retail_min",
                                     "retail_max"])
                    for price in date_dict.values():
                        writer.writerow([price.get('eng_date'), price.get('nep_date'), price.get('wholesale_min'),
                                         price.get('wholesale_max'), price.get('retail_min'),
                                         price.get('retail_max')])
