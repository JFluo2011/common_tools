import time
import random
import pprint
from multiprocessing import Pipe, Value, Array, JoinableQueue, Queue, current_process, Process, Manager, Pool
from multiprocessing.sharedctypes import typecode_to_type
from multiprocessing.managers import ListProxy, SyncManager


class IPCWithPipe(object):
    def send_message(self, child_conn):
        p = current_process()
        print('child id: {}'.format(id(p)))
        child_conn.send(['hello'])
        child_conn.close()

    def start(self):
        p = current_process()
        print('parent id: {}'.format(id(p)))
        parent_conn, child_conn = Pipe()
        p = Process(target=self.send_message, args=(child_conn, ))
        p.start()
        print(parent_conn.recv())
        p.join()


class IPCWithSharedMemory(object):
    def modify(self, value, array):
        p = current_process()
        print('child id: {}'.format(id(p)))
        value.value = -10
        array[-1] = 10

    def start(self):
        pprint.pprint('typecode_to_type: {}'.format(typecode_to_type))
        p = current_process()
        print('parent id: {}'.format(id(p)))
        value = Value('i', 10)
        array = Array('i', [1, 2, 3, 4, 5])
        p = Process(target=self.modify, args=(value, array, ))
        p.start()
        p.join()
        print(value.value, list(array))


class IPCWithQueue(object):
    def double(self, n):
        time.sleep(n)
        return n * 2

    def producer(self, tasks_queue):
        p = current_process()
        print('producer id: {}'.format(id(p)))
        wt = 0.0
        put_times = 0
        while True:
            if wt > 0.9:
                if not p.daemon:  # 如果不是守护进程，则设置consumer退出标志，并且主动退出进程
                    put_times += 1
                    tasks_queue.put(None)  # consumer退出标志
                    print('put_times: {}'.format(put_times))
                    print('stop producer')
                    break
                time.sleep(wt)
            else:
                put_times += 1
                wt = random.random()
                tasks_queue.put((self.double, wt))  # tasks_queue._unfinished_tasks.get_value() release times加1
                if p.daemon:
                    print('put_times: {}'.format(put_times))

    def consumer(self, tasks_queue, results_queue):
        p = current_process()
        print('consumer id: {}'.format(id(p)))
        task_done_times = 0
        while True:
            task = tasks_queue.get()
            task_done_times += 1
            if task is None and not p.daemon:  # 如果不是守护进程并且达到退出标志，则主动退出进程
                tasks_queue.task_done()
                print('task_done_times: {}'.format(task_done_times))
                print('stop consumer')
                break
            func, args = task
            results_queue.put(func(args))
            tasks_queue.task_done()  # tasks_queue._unfinished_tasks.get_value() acquire times加1
            if p.daemon:
                print('task_done_times: {}'.format(task_done_times))

    def start(self):
        # self.process_with_daemon()
        self.process_with_non_daemon()

    def process_with_daemon(self):
        t = current_process()
        print('parent id: {}'.format(id(t)))
        tasks_queue = JoinableQueue()
        results_queue = Queue()
        # daemon=True 当主进程结束时，自动关闭daemon==True的子进程, 子进程调用join()方法时该设置失效, 必须在子进程start之前设置
        producer = Process(target=self.producer, args=(tasks_queue,), daemon=True)
        consumer = Process(target=self.consumer, args=(tasks_queue, results_queue,), daemon=True)
        # producer.daemon = True
        # consumer.daemon = True
        producer.start()
        time.sleep(3)  # 保证tasks_queue._unfinished_tasks._semlock._count()不为0，否则tasks_queue.join()不会阻塞
        consumer.start()
        tasks_queue.join()  # 等待所有队列中的任务都执行完毕，即：tasks_queue._unfinished_tasks.get_value()==0，否则阻塞

    def process_with_non_daemon(self):
        p = current_process()
        print('parent id: {}'.format(id(p)))
        tasks_queue = JoinableQueue()
        results_queue = Queue()
        # daemon=True 当主进程结束时，自动关闭daemon==True的子进程, 子进程调用join()方法时该设置失效, 必须在子进程start之前设置
        producer = Process(target=self.producer, args=(tasks_queue,), daemon=False)
        consumer = Process(target=self.consumer, args=(tasks_queue, results_queue,), daemon=False)
        # producer.daemon = False
        # consumer.daemon = False
        producer.start()
        time.sleep(3)  # 保证tasks_queue._unfinished_tasks.get_value()不为0，否则tasks_queue.join()不会阻塞
        consumer.start()
        tasks_queue.join()  # 等待所有队列中的任务都执行完毕，即：tasks_queue._unfinished_tasks.get_value()==0，否则阻塞
        producer.join()  # 主进程等待producer结束才能结束
        consumer.join()  # 主进程等待consumer结束才能结束


class IPCWithManager(object):
    """
    一个multiprocessing.Manager对象会控制一个服务器进程，其他进程可以通过代理的方式来访问这个服务器进程。
    常见的共享方式有以下几种：
        Namespace。创建一个可分享的命名空间。
        Value/Array。和上面共享内存方式一样。
        dict/list。创建一个可分享的dict/list，支持对应数据结构的方法。
        Condition/Event/Lock/Queue/Semaphore。创建一个可分享的对应同步原语的对象。
    """
    def modify(self, ns, lst_proxy, dct_proxy):
        p = current_process()
        print('child id: {}'.format(id(p)))
        ns.value = -1
        lst_proxy[0] = 'a'
        dct_proxy['b'] = 'b'

    def start(self):
        p = current_process()
        print('parent id: {}'.format(id(p)))
        manager = Manager()
        ns = manager.Namespace()
        ns.a = 1
        lst_proxy = manager.list()
        lst_proxy.append(1)
        dct_proxy = manager.dict()
        dct_proxy['b'] = 2
        print(ns.a)
        print(lst_proxy)
        print(dct_proxy)
        p = Process(target=self.modify, args=(ns, lst_proxy, dct_proxy))
        p.start()
        p.join()
        print(ns.a)
        print(lst_proxy)
        print(dct_proxy)


class IPCWithDistributedManager(object):
    """
    运用managers搭建分布式进程间通信，C/S模式
    """
    # TODO: multiprocessing.managers 需要具体深入学习，现在模糊概念
    def func_server(self):
        p = current_process()
        print('{} id: {}'.format(p.name, id(p)))
        host = '127.0.0.1'
        port = 12345
        authkey = 'authkey'

        # SyncManager.register('get_list', list, proxytype=ListProxy)
        mgr = SyncManager(address=(host, port), authkey=authkey.encode('utf-8'))
        server = mgr.get_server()
        server.serve_forever()

    def func_client(self):
        p = current_process()
        print('{} id: {}'.format(p.name, id(p)))
        host = '127.0.0.1'
        port = 12345
        authkey = 'authkey'
        # SyncManager.register('get_list')
        mgr = SyncManager(address=(host, port), authkey=authkey.encode('utf-8'))
        mgr.connect()
        # fixme: 这里每个client得到的lst是独立的，以后深入了解为什么是独立的，如果要多个client共享一个lst怎么处理
        lst = mgr.list()
        while len(lst) <= 10:
            lst.append(random.random())
            print('{}: {}'.format(p.name, lst))
        # dct = mgr.dict()
        # index = 0
        # while len(dct.keys()) <= 10:
        #     dct.update({index: random.random()})
        #     index += 1
        #     print('{}: {}'.format(p.name, dct))

    def start(self):
        p = current_process()
        print('parent id: {}'.format(id(p)))
        server = Process(target=self.func_server, name='server', daemon=True)
        client_1 = Process(target=self.func_client, name='client_1')
        client_2 = Process(target=self.func_client, name='client_2')
        server.start()
        client_1.start()
        client_2.start()
        client_1.join()
        client_2.join()
        # p.join()


def ipc():
    """
    进程间通信（Interprocess communication）
        Queue(线程安全、进程安全)
        Value，Array（共享内存）
        Pipe（支持能被xmlrpclib序列化的对象）
        Manager（用于资源共享）
    """
    # obj = IPCWithPipe()
    # obj = IPCWithSharedMemory()
    # obj = IPCWithQueue()
    # obj = IPCWithManager()
    obj = IPCWithDistributedManager()
    obj.start()


class MultiProcessingPool(object):
    def __init__(self):
        self.args = range(25, 38)

    def fib(self, n):
        if n <= 2:
            return 1
        return self.fib(n - 1) + self.fib(n - 2)

    def start(self):
        start = time.time()
        pool_ = Pool(4)
        for num, result in zip(self.args, pool_.map(self.fib, self.args)):
            print('fib({}) = {}'.format(num, result))
        print('COST: {}'.format(time.time() - start))


def pool():
    obj = MultiProcessingPool()
    obj.start()


def synchronize():
    """
    同步机制(与线程相似)：
        Semaphore
        Lock(RLock)
        Condition
        Event
        Queue
    """
    pass


def main():
    # Pool进程池
    pool()
    # 同步机制(与线程相似)
    pass
    # 进程间通信（Interprocess communication）
    # ipc()


if __name__ == '__main__':
    main()
