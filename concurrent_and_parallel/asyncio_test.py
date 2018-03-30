import os
import re
import sys
import json
import random
import asyncio
import logging
from logging.handlers import RotatingFileHandler

import aiohttp
import async_timeout
import redis
from lxml import etree

logging.getLogger('asyncio').setLevel(logging.DEBUG)

FORMAT = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
DATEFMT = '%a, %d %b %Y %H:%M:%S'


def setup_log(level, file_path, max_bytes=20 * 1024 * 1024, backup_count=5):
    if not os.path.exists(os.path.split(file_path)[0]):
        os.makedirs(os.path.split(file_path)[0])
    logging.basicConfig(level=level,
                        format=FORMAT,
                        datefmt=DATEFMT)
    rotate_handler = RotatingFileHandler(file_path, maxBytes=max_bytes, backupCount=backup_count)
    rotate_handler.setLevel(level)
    rotate_handler.setFormatter(logging.Formatter(FORMAT, DATEFMT))
    logging.getLogger('').addHandler(rotate_handler)


class Crawler(object):
    def __init__(self, max_tries=4, max_tasks=10, *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        }
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.start_page_key = 'start_page'
        self.detail_page_key = 'detail_page'
        self.download_page_key = 'download_page'

    def get_proxy(self):
        try:
            proxy = random.choice(self.redis_client.keys('http://*'))
        except:
            return None
        return proxy

    async def close(self):
        await self.session.close()

    async def parse_page(self):
        while True:
            await asyncio.sleep(3)
            url = self.redis_client.spop(self.start_page_key)
            if url is None:
                # logging.info('no more start page to parse')
                await asyncio.sleep(10)
                continue
            response = await self.fetch(url, self.start_page_key, url)
            if response is not None:
                if response.status == 200:
                    try:
                        async with async_timeout.timeout(10):
                            text = await response.text()
                    except Exception as err:
                        logging.error(str(err))
                        self.loop.run_in_executor(None, self.insert_task, self.start_page_key, url)
                    else:
                        selector = etree.HTML(text)
                        for sel in selector.xpath('//*[@class="gallery_image"]'):
                            xpath_ = './/img[@class="img-responsive img-rounded"]/@src'
                            category = re.findall('ua/(.*?)/page', url)[0]
                            image_dir, image_number = re.findall('/mini/(\d+)/(\d+)\.jpg', sel.xpath(xpath_)[0])[0]
                            json_data = {
                                'url': sel.xpath('./@href')[0],
                                'image_number': image_number,
                                'image_dir': image_dir,
                                'category': category,
                            }
                            self.loop.run_in_executor(None, self.insert_task,
                                                      self.detail_page_key, json.dumps(json_data))
                        # logging.info('{} {}'.format('parse_page', url))
                else:
                    logging.warning('{}: {}'.format(url, response.status))

    async def parse_detail_page(self):
        base_url = 'https://look.com.ua/pic'
        while True:
            await asyncio.sleep(3)
            value = self.redis_client.spop(self.detail_page_key)
            if value is None:
                # logging.info('no more detail page to parse')
                await asyncio.sleep(10)
                continue
            json_data = json.loads(value)
            url = json_data['url']
            category = json_data['category']
            image_number = json_data['image_number']
            image_dir = json_data['image_dir']
            response = await self.fetch(url, self.detail_page_key, value)
            if response is not None:
                if response.status == 200:
                    try:
                        async with async_timeout.timeout(10):
                            text = await response.text()
                    except Exception as err:
                        logging.error(str(err))
                        self.loop.run_in_executor(None, self.insert_task, self.detail_page_key, value)
                    else:
                        selector = etree.HTML(text)
                        for url in selector.xpath('//*[@class="llink list-inline"]/li/a/@href'):
                            resolution = re.findall(r'download/\d+/(\d+x\d+)/', url)[0]
                            path = os.path.join(os.path.abspath('.'), 'images', category, image_number, resolution + '.jpg')
                            url = '/'.join([base_url, image_dir, resolution, 'look.com.ua-' + image_number + '.jpg'])
                            if os.path.exists(path):
                                logging.info('image {} already downloaded'.format(path))
                                continue
                            json_data = {
                                'url': url,
                                'path': path,
                            }
                            self.loop.run_in_executor(None, self.insert_task, self.download_page_key,
                                                      json.dumps(json_data))
                        # logging.info('{} {}'.format('parse_detail_page', url))
                else:
                    logging.warning('{}: {}'.format(url, response.status))

    def insert_task(self, redis_key, value):
        self.redis_client.sadd(redis_key, value)

    async def fetch(self, url, key, value):
        try:
            response = await self.session.get(url, headers=self.headers, ssl=False, timeout=30,
                                              allow_redirects=False, proxy=self.get_proxy())
        except Exception as err:
            logging.warning('{} raised {}'.format(url, str(err)))
            self.loop.run_in_executor(None, self.insert_task, key, value)
            return None
        else:
            return response

    def save_image(self, path, content):
        if not os.path.exists('\\'.join(path.split('\\')[:-1])):
            os.makedirs('\\'.join(path.split('\\')[:-1]))
        with open(path, 'wb') as f:
            f.write(content)
        logging.info('{}: downloaded'.format(path))

    async def download_image(self):
        while True:
            await asyncio.sleep(1)
            value = self.redis_client.spop(self.download_page_key)
            if value is None:
                logging.info('no more download page to parse')
                await asyncio.sleep(10)
                continue
            json_data = json.loads(value)
            url = json_data['url']
            path = json_data['path']
            response = await self.fetch(url, self.download_page_key, value)
            if response is not None:
                if response.status == 200:
                    try:
                        async with async_timeout.timeout(10):
                            content = await response.read()
                    except Exception as err:
                        logging.error(str(err))
                        self.loop.run_in_executor(None, self.insert_task, self.download_page_key, value)
                    else:
                        self.loop.run_in_executor(None,  self.save_image, path, content)
                else:
                    logging.warning('{}: {}'.format(url, response.status))

    async def crawl(self):
        """Run the crawler until all finished."""
        step = self.max_tasks // 3
        workers = []
        workers.extend([asyncio.Task(self.parse_page(), loop=self.loop) for _ in range(self.max_tasks)[:step]])
        workers.extend([asyncio.Task(self.parse_detail_page(), loop=self.loop) for _ in range(self.max_tasks)[step:step*2]])
        workers.extend([asyncio.Task(self.download_image(), loop=self.loop) for _ in range(10)])
        while True:
            await asyncio.sleep(60)
        # for w in workers:
        #     w.cancel()


def main():
    setup_log(logging.INFO, os.path.join(os.path.abspath('.'), 'logs', 'look_ua.log'))
    source_urls = [
        # ('https://www.look.com.ua/love/page/{}/', 42),
        # ('https://www.look.com.ua/spring/page/{}/', 94),
        # ('https://www.look.com.ua/autumn/page/{}/', 99),
        # ('https://www.look.com.ua/hi-tech/page/{}/', 114),
        # ('https://www.look.com.ua/summer/page/{}/', 119),
        # ('https://www.look.com.ua/newyear/page/{}/', 156),
        # ('https://www.look.com.ua/men/page/{}/', 157),
        # ('https://www.look.com.ua/holidays/page/{}/', 159),
        # ('https://www.look.com.ua/creative/page/{}/', 168),
        # ('https://www.look.com.ua/winter/page/{}/', 172),
        # ('https://www.look.com.ua/situation/page/{}/', 172),
        # ('https://www.look.com.ua/music/page/{}/', 184),
        # ('https://www.look.com.ua/food/page/{}/', 211),
        # ('https://www.look.com.ua/weapon/page/{}/', 217),
        # ('https://www.look.com.ua/aviation/page/{}/', 261),
        # ('https://www.look.com.ua/textures/page/{}/', 267),
        # ('https://www.look.com.ua/minimalism/page/{}/', 278),
        # ('https://www.look.com.ua/movies/page/{}/', 280),
        # ('https://www.look.com.ua/3d/page/{}/', 286),
        # ('https://www.look.com.ua/abstraction/page/{}/', 293),
        # ('https://www.look.com.ua/space/page/{}/', 302),
        # ('https://www.look.com.ua/sport/page/{}/', 307),
        # ('https://www.look.com.ua/mood/page/{}/', 422),
        # ('https://www.look.com.ua/flowers/page/{}/', 595),
        # ('https://www.look.com.ua/macro/page/{}/', 636),
        # ('https://www.look.com.ua/travel/page/{}/', 674),
        # ('https://www.look.com.ua/fantasy/page/{}/', 687),
        # ('https://www.look.com.ua/anime/page/{}/', 694),
        # ('https://www.look.com.ua/games/page/{}/', 720),
        # ('https://www.look.com.ua/other/page/{}/', 778),
        # ('https://www.look.com.ua/animals/page/{}/', 1103),
        # ('https://www.look.com.ua/landscape/page/{}/', 1140),
        # ('https://www.look.com.ua/nature/page/{}/', 1142),
        # ('https://www.look.com.ua/auto/page/{}/', 1559),
        # ('https://www.look.com.ua/girls/page/{}/', 9266),
    ]
    loop = asyncio.get_event_loop()
    crawler = Crawler(max_tries=5, max_tasks=30)
    for info in source_urls:
        for i in range(1, info[1]+1):
            crawler.insert_task(crawler.start_page_key, info[0].format(i))

    try:
        loop.run_until_complete(crawler.crawl())  # Crawler gonna crawl.
    except KeyboardInterrupt:
        sys.stderr.flush()
        logging.warning('\nInterrupted\n')
    finally:
        loop.run_until_complete(crawler.close())

        # next two lines are required for actual aiohttp resource cleanup
        loop.stop()
        loop.run_forever()

        loop.close()


if __name__ == '__main__':
    main()
