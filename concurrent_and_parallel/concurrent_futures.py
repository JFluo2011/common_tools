import time
# ThreadPoolExecutor和ProcessPoolExecutor分别是对threading和multiprocessing的封装，python2：futures(第三方库)
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from multiprocessing import Pool


class ConcurrentFutures(object):
    def __init__(self):
        self.args = range(25, 38)
        self.kw = {k: self.fib for k in range(25, 38)}  # 可设置不同函数作为value

    def fib(self, n):
        if n <= 2:
            return 1
        return self.fib(n - 1) + self.fib(n - 2)

    def func_map(self):
        start = time.time()
        # ThreadPoolExecutor 和 ProcessPoolExecutor用法一样，所以根据应用场景选择
        with ProcessPoolExecutor(max_workers=4) as executor:
            for num, result in zip(self.args, executor.map(self.fib, self.args)):
                print('fib({}) = {}'.format(num, result))
        print('COST: {}'.format(time.time() - start))

    def func_submit(self):
        start = time.time()
        # ThreadPoolExecutor 和 ProcessPoolExecutor用法一样，所以根据应用场景选择
        with ProcessPoolExecutor(max_workers=4) as executor:
            future_to_num = {executor.submit(func, k): k for k, func in self.kw.items()}
            for future in as_completed(future_to_num):
                num = future_to_num[future]
                result = future.result()
                print('fib({}) = {}'.format(num, result))
        print('COST: {}'.format(time.time() - start))

    @staticmethod
    def func(x):
        r = 0
        k = 50
        for k in range(1, k + 2):
            r += x ** (1 / k ** 1.5)
        return r

    def multiprocessing_pool_vs_concurrent_futures(self):
        """
        multiprocessing.pool 最快，批量提交任务，可以节省IPC(进程间通信)开销
        concurrent.futures的ProcessPoolExecutor是对multiprocessing， threading的封装，所以速度稍慢
        python2中没有chunksize参数，每次只提交一个任务，所以速度极慢
        python3中有chunksize参数，批量提交chunksize个任务，所以只比multiprocessing.pool慢在封装层
        multiprocessing.pool与concurrent.futures的比较详见 http://www.dongwm.com/的文章《使用Python进行并发编程-PoolExecutor篇》
        """
        args = range(1, 100000)
        print('multiprocessing.pool.Pool:\n')
        start = time.time()
        l = []
        pool = Pool(4)
        for num, result in zip(args, pool.map(self.func, args)):
            l.append(result)
        print(len(l))
        print('COST: {}'.format(time.time() - start))
        print('ProcessPoolExecutor without chunksize:\n')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            for num, result in zip(args, executor.map(self.func, args)):
                l.append(result)
        print(len(l))
        print('COST: {}'.format(time.time() - start))
        print('ProcessPoolExecutor with chunksize:\n')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            # 保持和multiprocessing.chunk_size
            chunk_size, extra = divmod(len(args), executor._max_workers * 4)
            for num, result in zip(args, executor.map(self.func, args, chunksize=chunk_size)):
                l.append(result)
        print(len(l))
        print('COST: {}'.format(time.time() - start))

    def start(self):
        # self.func_map()
        # self.func_submit()
        self.multiprocessing_pool_vs_concurrent_futures()


def main():
    obj = ConcurrentFutures()
    obj.start()


if __name__ == '__main__':
    main()
