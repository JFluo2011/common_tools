import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import requests
import aiohttp


class Download(object):
    def __init__(self):
        self.numbers = range(40)
        self.base_url = 'http://httpbin.org/get?a={}'

    def fetch(self, a):
        r = requests.get(self.base_url.format(a))
        return r.json()['args']['a']

    def download_with_requests_threadpoolexecutor(self):
        start = time.time()
        with ThreadPoolExecutor(max_workers=4) as executor:
            for num, result in zip(self.numbers, executor.map(self.fetch, self.numbers)):
                # print('fetch({}) = {}'.format(num, result))
                pass
        print('Use requests+ThreadPoolExecutor cost: {}'.format(time.time() - start))

    def download_with_requests_processpoolexecutor(self):
        start = time.time()
        with ProcessPoolExecutor(max_workers=4) as executor:
            for num, result in zip(self.numbers, executor.map(self.fetch, self.numbers)):
                # print('fetch({}) = {}'.format(num, result))
                pass
        print('Use requests+ProcessPoolExecutor cost: {}'.format(time.time() - start))

    async def run_scraper_tasks(self, executor):
        loop = asyncio.get_event_loop()
        tasks = []
        for number in self.numbers:
            task = loop.run_in_executor(executor, self.fetch, number)
            task.__num = number
            tasks.append(task)
        completed, pending = await asyncio.wait(tasks)
        results = {t.__num: t.result() for t in completed}
        # for num, result in sorted(results.items(), key=lambda x: x[0]):
        #     print('fetch({}) = {}'.format(num, result))

    def download_with_requests_asyncio_threadpoolexecutor(self):
        start = time.time()
        executor = ThreadPoolExecutor(max_workers=4)
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.run_scraper_tasks(executor))
        print('Use asyncio+requests+ThreadPoolExecutor cost: {}'.format(time.time() - start))

    async def fetch_async(self, a):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url.format(a)) as r:
                data = await r.json()
        return a, data['args']['a']

    def download_with_aiohttp_asyncio(self):
        start = time.time()
        tasks = [self.fetch_async(num) for num in self.numbers]
        event_loop = asyncio.get_event_loop()
        results = event_loop.run_until_complete(asyncio.gather(*tasks))
        # for num, result in zip(self.numbers, results):
        #     print('fetch({}) = {}'.format(num, result[1]))
        print('Use asyncio+aiohttp cost: {}'.format(time.time() - start))

    def sub_loop(self, numbers):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = [self.fetch_async(num) for num in numbers]
        results = loop.run_until_complete(asyncio.gather(*tasks))
        # for num, result in results:
        #     print('fetch({}) = {}'.format(num, result))

    async def run(self, executor, numbers):
        await asyncio.get_event_loop().run_in_executor(executor, self.sub_loop, numbers)

    def chunks(self, l, size):
        n = len(l) // size
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def download_with_aiohttp_asyncio_threadpoolexecutor(self):
        start = time.time()
        executor = ThreadPoolExecutor(max_workers=4)
        event_loop = asyncio.get_event_loop()
        tasks = [self.run(executor, chunked) for chunked in self.chunks(self.numbers, 4)]
        results = event_loop.run_until_complete(asyncio.gather(*tasks))
        print('Use asyncio+aiohttp+ThreadPoolExecutor cost: {}'.format(time.time() - start))

    def start(self):
        time.sleep(1)
        self.download_with_requests_threadpoolexecutor()
        time.sleep(1)
        self.download_with_requests_processpoolexecutor()
        time.sleep(1)
        self.download_with_requests_asyncio_threadpoolexecutor()
        time.sleep(1)
        self.download_with_aiohttp_asyncio()
        time.sleep(1)
        self.download_with_aiohttp_asyncio_threadpoolexecutor()


def main():
    obj = Download()
    obj.start()


if __name__ == '__main__':
    main()
