import csv
import re

import scrapy
import json
from scrapy.selector import Selector
from bikram import samwat, convert_bs_to_ad
from datetime import date


class NamisSpider(scrapy.Spider):
    name = "namis"
    token_url = 'http://namis.gov.np/pages/market/marketPriceForm/'
    market_url = 'https://api.myjson.com/bins/h2ayn/'
    request_url = 'http://namis.gov.np/site/pages/ajax_priceList/'
    commodity_by_category_url = 'http://namis.gov.np/site/pages/ajax_getCommodityByCategory/'
    headers = {"X-Requested-With": "XMLHttpRequest"}

    def jsonify_response(self, response):
        response_unicode = response.body_as_unicode()
        return json.loads(response_unicode)

    def parse_option_tags(self, option_tags):
        for index, option_tag in enumerate(option_tags):
            id = option_tag.css(f'option:nth-child({index+1})::attr(value)').get()
            if id != "" or len(id) > 0:
                name = option_tag.css(f'option:nth-child({index+1})::text').get()
                option_data = {'id': id, 'name': name}
                yield option_data

    def start_requests(self):
        yield scrapy.Request(url=self.token_url, callback=self.parse_token)

    def parse_token(self, response):
        self.token = response.css('input[name="csrf_appcore"]::attr(value)').get()
        category_option_list = response.css('select#CATEGORY option')
        self.category = list(self.parse_option_tags(category_option_list))
        market_option_list = response.css('select#MARKET option')
        self.markets = list(self.parse_option_tags(market_option_list))
        market_match = list(filter(lambda x: re.search(self.name, x['name'].lower()), self.markets))
        if market_match:
            self.matched_market = market_match[0]
            for category in self.category:
                data = {
                    'csrf_appcore': self.token,
                    'cat': category['id']
                }
                yield scrapy.FormRequest(url=self.commodity_by_category_url, headers=self.headers, formdata=data,
                                         callback=self.parse_commodity, meta={'category': category})
        else:
            return

    def parse_commodity(self, response):
        category = response.meta.get('category')
        jsonified_response = self.jsonify_response(response)
        data = jsonified_response.get('data', None)
        if data:
            html_response = Selector(text=data)
            commodity_list = list(self.parse_option_tags(html_response.css('option')))

            for commodity in commodity_list:
                data = self.get_request_data(self.matched_market, commodity, category)
                yield scrapy.FormRequest(url=self.request_url, headers=self.headers, formdata=data,
                                         callback=self.parse_market_price, meta={'commodity': commodity})

    def get_request_data(self, market, commodity, category):
        data = {
            'csrf_appcore': self.token,
            'market': market['id'],
            'category': category['id'],
            'commodity': commodity['id'],
            'date_from': '2001-01-01',
            'date_to': str(samwat.from_ad(date.today()))
        }
        return data

    def parse_market_price(self, response):
        commodity = response.meta.get('commodity')
        jsonified_response = self.jsonify_response(response)
        html_response = Selector(text=jsonified_response['data']['data'])
        response_data = html_response.css("tbody tr")
        if response_data:
            filename = f"{self.name} {commodity['name'].replace('/', '')}.csv"
            with open(filename, "w") as file:
                writer = csv.writer(file)
                writer.writerow(["eng_date", "nep_date", "wholesale_min", "wholesale_max", "retail_min", "retail_max"])
                for dat in response_data:
                    row_values = dat.css('td::text').getall()

                    nep_date = row_values[3]
                    nep_date_list = nep_date.split('-')
                    eng_date = convert_bs_to_ad(samwat(int(nep_date_list[0]), int(nep_date_list[1]), int(nep_date_list[2])))

                    wholesale_min = row_values[4]
                    wholesale_max = row_values[5]
                    retail_min = row_values[6]
                    retail_max = row_values[7]

                    writer.writerow([eng_date, nep_date, wholesale_min, wholesale_max, retail_min, retail_max])
