import json
import re
from fake_useragent import UserAgent
import scrapy
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
from scrapy.item import Item, Field

useragent = UserAgent()
mongo_link = 'mongodb+srv://user:Password123@cluster0.raiho.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
cluster = MongoClient(
    f"{mongo_link}")
db = cluster["testdata"]
collection = db["testcollection"]


class CustomItem(Item):
    id_factory = Field()
    name_factory = Field()
    model = Field()
    small = Field()
    group = Field()
    group_2 = Field()
    group_3 = Field()
    name_part = Field()
    number_part = Field()


s = {
    'id_factory',
    'name_factory',
    'model',
    'small',
    'group',
    'group_2',
    'group_3'
}
# id_factory: rsm,
# name_factory: ооо «комбайновый завод «РОСТСЕЛЬМАШ»,
# model: Дон 1500б,
# small: 1500б,
# group: кабина,
# group_2: кабина,
# group_3: дверь кабины,
# number_part: рсм-10.04,
# name_part: каркас двери}


process = CrawlerProcess({
    'USER_AGENT': f'{useragent.random}',
})


class TovarsSpider(scrapy.Spider):
    name = 'tovars'
    allowed_domains = ['konsulavto.ru']
    start_urls = ['http://konsulavto.ru/']

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)

        # self.id_factory = None
        # self.name_factory = None
        # self.model = None
        # self.small = None
        # self.group = None
        # self.group_2 = None
        # self.group_3 = None
        # self.number_part = None
        # self.name_part = None

    def start_requests(self):
        url = 'https://www.konsulavto.ru/acat/farm'
        yield scrapy.Request(url, callback=self.parse_url)

    def parse_url(self, response, **kwargs):

        urls = response.xpath('//*[@id="content"]/div[1]/div[1]/ul//li//@href').getall()
        urls = urls[::2]
        for comp in urls:
            comp = 'https://www.konsulavto.ru' + comp  # https://www.konsulavto.ru/acat/farm/rsm
            id_factory_data = comp.split('/')[-1]  # id_factory
            url = response.urljoin(comp)
            yield scrapy.Request(url, callback=self.parse_comp, meta={'id_factory': id_factory_data})

    def parse_comp(self, response, **kwargs):
        name = response.xpath("//div[@class='acat-path']/h2/text()").get()
        name_factory_data = name.replace(' - каталог автозапчастей', '').replace('Компания ', '')  # name_factory
        for href in response.css('.acat-model .left::attr("href")').extract():
            model = response.xpath(f'//div[@class="acat-model-descr-box"]/a[contains(@href,"{href}")]/text()').get()
            model_data = re.sub(r'[\t\n\r]', '', model)
            small = response.xpath(
                f'//div[@class="acat-model-descr-box"]/a[contains(@href,"{href}")]/following::small/text()').get()
            small_data = re.sub(r'[\t\n\r]', '', small)
            href = 'https://www.konsulavto.ru' + href  # https://www.konsulavto.ru/acat/farm/rsm/don-obshhijj-katalog
            url = response.urljoin(href)
            id_factory_data = response.request.meta['id_factory']
            yield scrapy.Request(url, callback=self.parse_car, meta={
                'id_factory': id_factory_data,
                'name_factory': name_factory_data,
                'model': model_data,
                'small': small_data,
            })

    def parse_car(self, response, **kwargs):
        all_cards = response.xpath("//div[@class='level-0']/a/text()").getall()
        for el in all_cards:
            level0 = response.xpath(
                f"//div[@class='level-0']/a[text()='{el}']/../following::div[1]/div[@class='level-1']/a/text()").getall()
            group_data = ' '.join(el.split())
            for small in level0:
                group_2_data = ' '.join(small.split())
                level2 = response.xpath(
                    f'//div[@class="level-1"]/a[contains(text(),"{small}")]/../following::div[1]/div/a').getall()
                for level2_name in level2:
                    r = r'(?<=href\=\")[\w:\-\=\/\:\d\?\.\#]*'
                    level2href = re.findall(r, level2_name)[0]
                    url = 'https://www.konsulavto.ru' + level2href
                    r1 = r'<(a).*?>'
                    r2 = r'<(.a).*?>'
                    r_text = re.sub(r1, '', level2_name)
                    group_3_data = re.sub(r2, '', r_text)
                    id_factory_data = response.request.meta['id_factory']
                    model_data = response.request.meta['model']
                    small_data = response.request.meta['small']
                    name_factory_data = response.request.meta['name_factory']
                    yield scrapy.Request(url, callback=self.parse_detail, meta={
                        'id_factory': id_factory_data,
                        'model': model_data,
                        'small': small_data,
                        'name_factory': name_factory_data,
                        'group': group_data,
                        'group_2': group_2_data,
                        'group_3': group_3_data,
                    })

    def parse_detail(self, response, **kwargs):
        rows_num = response.xpath("//table[@class='partsTable_ac']/tr/td[3]/text()").getall()
        rows_name = response.xpath("//table[@class='partsTable_ac']/tr/td[3]/following::td[1]/text()").getall()
        r1 = []
        r2 = []

        for el in rows_num:
            r2.append(re.sub(r'[\t\n\r]', '', el))
        for el in rows_name:
            el1 = el.strip()
            if el1 != '':
                r1.append(el1)

        for i in range(len(r1)):
            item = CustomItem()
            name_part = r1[i]
            number_part = r2[i]
            item['id_factory'] = response.request.meta['id_factory']
            item['model'] = response.request.meta['model']
            item['small'] = response.request.meta['small']
            item['group'] = response.request.meta['group']
            item['group_2'] = response.request.meta['group_2']
            item['group_3'] = response.request.meta['group_3']
            item['name_part'] = name_part
            item['number_part'] = number_part
            collection.insert_one({
                'id_factory': item['id_factory'],
                'model': item['model'],
                'small': item['small'],
                'group': item['group'],
                'group_2': item['group_2'],
                'group_3': item['group_3'],
                'name_part': item['name_part'],
                'number_part': item['number_part'],
            })
            # s = {
            #     'id_factory': item['name_factory']
            # 'name_factory': self.name_factory,
            # 'model': self.model,
            # 'small': self.small,
            # 'group': self.group,
            # 'group_2': self.group_2,
            # 'group_3': None
            # }

            # collection.insert_one(s)
            # with open(f"data.json", "a", encoding='utf-8') as file:
            #     json.dump(item, file, indent=4, ensure_ascii=False)
            yield item


if __name__ == '__main__':
    process.crawl(TovarsSpider)
    process.start()
