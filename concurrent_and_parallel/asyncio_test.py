import os
import re
import sys
import json
import random
import asyncio
from asyncio import Queue
import logging
from logging.handlers import RotatingFileHandler

import aiohttp
import async_timeout
import redis
from lxml import etree

# logging.getLogger('asyncio').setLevel(logging.DEBUG)

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
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.start_page_key = 'start_page'
        self.detail_page_key = 'detail_page'
        self.download_page_key = 'download_page'
        # self.download_semaphore = asyncio.Semaphore(40)
        self.semaphore = asyncio.Semaphore(10)
        self.handle_failed_semaphore = asyncio.Semaphore(10)
        self.q = Queue(loop=self.loop)

    async def start_task(self):
        while True:
            await self.create_task(self.start_page_key, self.parse_detail_task)

    async def detail_task(self):
        while True:
            await self.create_task(self.detail_page_key, self.parse_download_task)

    async def download_task(self):
        while True:
            # async with self.download_semaphore:
            task, json_data = await self.get_task(self.download_page_key)
            if task is None:
                await asyncio.sleep(10)
                continue
            response = await self.fetch(json_data['url'], self.download_page_key, task)
            if (response is not None) and (response.status == 200):
                try:
                    async with async_timeout.timeout(10):
                        content = await response.read()
                except Exception as err:
                    logging.error(str(err))
                    self.loop.run_in_executor(None, self.insert_task, self.download_page_key, task)
                    # self.q.put_nowait((self.download_page_key, task))
                else:
                    self.loop.run_in_executor(None,  self.save_image, json_data['path'], content)

    async def handle_failed_task(self):
        while True:
            async with self.handle_failed_semaphore:
                redis_key, task = await self.q.get()
                self.loop.run_in_executor(None, self.insert_task, redis_key, task)
                logging.info('handle failed task: {}'.format(task))
                self.q.task_done()

    def get_proxy(self):
        try:
            proxy = random.choice(self.redis_client.keys('http://*'))
        except:
            return None
        return proxy

    async def close(self):
        await self.session.close()

    async def get_task(self, redis_key):
        task = self.redis_client.spop(redis_key)
        return task, (task is not None) and json.loads(task)

    async def parse_task(self, redis_key, task, json_data, response, operate_func):
        try:
            async with async_timeout.timeout(10):
                text = await response.text()
        except Exception as err:
            logging.error(str(err))
            self.loop.run_in_executor(None, self.insert_task, redis_key, task)
            # self.q.put_nowait((redis_key, task))
        else:
            await operate_func(text, json_data)

    async def parse_detail_task(self, text, json_data):
        selector = etree.HTML(text)
        for sel in selector.xpath('//*[@class="gallery_image"]'):
            xpath_ = './/img[@class="img-responsive img-rounded"]/@src'
            category = re.findall('ua/(.*?)/page', json_data['url'])[0]
            image_dir, image_number = re.findall('/mini/(\d+)/(\d+)\.jpg', sel.xpath(xpath_)[0])[0]
            meta = {
                'url': sel.xpath('./@href')[0],
                'image_number': image_number,
                'image_dir': image_dir,
                'category': category,
            }
            self.loop.run_in_executor(None, self.insert_task, self.detail_page_key, json.dumps(meta))

    async def parse_download_task(self, text, json_data):
        base_url = 'https://look.com.ua/pic'
        selector = etree.HTML(text)
        for url in selector.xpath('//*[@class="llink list-inline"]/li/a/@href'):
            resolution = re.findall(r'download/\d+/(\d+x\d+)/', url)[0]
            path = os.path.join(os.path.abspath('.'), 'images', json_data['category'],
                                json_data['image_number'], resolution + '.jpg')
            url = '/'.join([base_url, json_data['image_dir'], resolution,
                            'look.com.ua-' + json_data['image_number'] + '.jpg'])
            if os.path.exists(path):
                logging.info('image {} already downloaded'.format(path))
                continue
            meta = {'url': url, 'path': path, }
            self.loop.run_in_executor(None, self.insert_task, self.download_page_key, json.dumps(meta))

    async def create_task(self, redis_key, operate_func):
        async with self.semaphore:
            task, json_data = await self.get_task(redis_key)
            if task is None:
                await asyncio.sleep(10)
            else:
                url = json_data['url']
                response = await self.fetch(url, redis_key, task)
                if (response is not None) and (response.status == 200):
                    await self.parse_task(redis_key, task, json_data, response, operate_func)

    def insert_task(self, redis_key, task):
        self.redis_client.sadd(redis_key, task)

    async def fetch(self, url, key, value):
        logging.info('active tasks count: {}'.format(len(asyncio.Task.all_tasks())))
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

    async def crawl(self):
        """Run the crawler until all finished."""
        # step = self.max_tasks // 3
        workers = []
        workers.extend([asyncio.Task(self.start_task(), loop=self.loop) for _ in range(2)])
        workers.extend([asyncio.Task(self.detail_task(), loop=self.loop) for _ in range(5)])
        workers.extend([asyncio.Task(self.download_task(), loop=self.loop) for _ in range(40)])
        # asyncio.Task(self.start_task(), loop=self.loop)
        # asyncio.Task(self.detail_task(), loop=self.loop)
        # asyncio.Task(self.download_task(), loop=self.loop)
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
            json_data = {
                'url': info[0].format(i),
            }
            crawler.insert_task(crawler.start_page_key, json.dumps(json_data))

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
