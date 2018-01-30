import time
import random
import queue
import threading


class SynchronizeWithSemaphore(object):
    def __init__(self):
        self.semaphore = threading.Semaphore(3)

    def start(self):
        self.create_threads()

    def create_threads(self):
        thread_lst = []
        for i in range(1, 6):
            t = threading.Thread(name='thread {}'.format(i), target=self.target, args=(i, ))
            t.start()
            thread_lst.append(t)

        # for t in thread_lst:
        #     t.join()

    def target(self, *args):
        t = threading.current_thread()
        with self.semaphore:
            print('{} acquire semaphore {}'.format(t.name, args))
            time.sleep(random.randint(1, 3))
        print('{} release semaphore'.format(t.name))


class SynchronizeWithLock(object):
    def __init__(self):
        self.lock = threading.Lock()

    def start(self):
        self.create_threads()

    def create_threads(self):
        thread_lst = []
        for i in range(1, 6):
            t = threading.Thread(name='thread {}'.format(i), target=self.target, args=(i, ))
            t.start()
            thread_lst.append(t)

        for t in thread_lst:
            t.join()

    def target(self, *args):
        t = threading.current_thread()
        with self.lock:
            print('{} acquire lock {}'.format(t.name, args))
            time.sleep(random.randint(1, 3))
        print('{} release lock'.format(t.name))


class SynchronizeWithCondition(object):
    def __init__(self):
        self.condition = threading.Condition()
        self.count = 0

    def start(self):
        self.create_threads()

    def create_threads(self):
        thread_lst = []
        t = threading.Thread(name='thread producer', target=self.producer)
        t.start()
        thread_lst.append(t)

        for i in range(1, 3):
            time.sleep(1)
            t = threading.Thread(name='thread consumer-{}'.format(i), target=self.consumer)
            t.start()
            thread_lst.append(t)

        for t in thread_lst:
            t.join()

    def consumer(self):
        t = threading.current_thread()
        while True:
            if self.count < 0:
                time.sleep(1)
                continue
            with self.condition:
                self.condition.wait()
                self.count -= 1
                print('{}: goods remaining {}'.format(t.name, self.count))
            time.sleep(random.randint(1, 3))

    def producer(self):
        t = threading.current_thread()
        while True:
            with self.condition:
                if self.count <= 95:
                    self.count += 10
                    print('{}: producer goods'.format(t.name))
                self.condition.notify_all()
            time.sleep(2)


class SynchronizeWithEvent(object):
    def __init__(self):
        self.event = threading.Event()
        self.count = 0

    def start(self):
        self.create_threads()

    def create_threads(self):
        thread_lst = []
        t = threading.Thread(name='thread producer', target=self.producer)
        t.start()
        thread_lst.append(t)

        for i in range(1, 3):
            time.sleep(1)
            t = threading.Thread(name='thread consumer-{}'.format(i), target=self.consumer)
            t.start()
            thread_lst.append(t)

        for t in thread_lst:
            t.join()

    def consumer(self):
        t = threading.current_thread()
        while True:
            if self.count < 0:
                time.sleep(1)
                continue
            if self.event.wait():
                self.count -= 1
                self.event.clear()
                print('{}: goods remaining {}'.format(t.name, self.count))
                time.sleep(random.randint(1, 3))

    def producer(self):
        t = threading.current_thread()
        while True:
            if self.count <= 95:
                self.count += 10
                print('{}: producer goods'.format(t.name))
            else:
                self.event.set()
            time.sleep(2)


class SynchronizeWithQueue(object):
    def double(self, n):
        time.sleep(n)
        return n * 2

    def producer(self, tasks_queue):
        t = threading.current_thread()
        print('producer id: {}'.format(id(t)))
        wt = 0.0
        while True:
            if wt > 0.9:
                if not t.daemon:  # 如果不是守护线程，则设置consumer退出标志，并且主动退出线程
                    tasks_queue.put(None)  # consumer退出标志
                    print('stop producer')
                    break
                time.sleep(wt)
            else:
                wt = random.random()
                tasks_queue.put((self.double, wt))  # tasks_queue.unfinished_tasks += 1

    def consumer(self, tasks_queue, results_queue):
        t = threading.current_thread()
        print('consumer id: {}'.format(id(t)))
        while True:
            task = tasks_queue.get()
            if task is None and not t.daemon:  # 如果不是守护线程并且达到退出标志，则主动退出线程
                tasks_queue.task_done()
                print('stop consumer')
                break
            func, args = task
            results_queue.put(func(args))
            tasks_queue.task_done()  # tasks_queue.unfinished_tasks -= 1

    def start(self):
        # self.daemon()
        self.non_daemon()

    def daemon(self):
        t = threading.current_thread()
        print('parent id: {}'.format(id(t)))
        tasks_queue = queue.Queue()
        results_queue = queue.Queue()
        # daemon=True 当主线程结束时，自动关闭daemon==True的子线程, 子线程调用join()方法时该设置失效, 可以随后设置，但必须在子线程start之前
        producer = threading.Thread(target=self.producer, args=(tasks_queue,), daemon=True)
        consumer = threading.Thread(target=self.consumer, args=(tasks_queue, results_queue,), daemon=True)
        producer.start()
        time.sleep(1)  # 保证unfinished_tasks不为0，否则tasks_queue.join()不会阻塞
        consumer.start()
        tasks_queue.join()  # 等待所有队列中的任务都执行完毕，即：tasks_queue.unfinished_tasks==0，否则阻塞

    def non_daemon(self):
        t = threading.current_thread()
        print('parent id: {}'.format(id(t)))
        tasks_queue = queue.Queue()
        results_queue = queue.Queue()
        # daemon=True 当主线程结束时，自动关闭daemon==True的子线程, 子线程调用join()方法时该设置失效, 可以随后设置，但必须在子线程start之前
        producer = threading.Thread(target=self.producer, args=(tasks_queue,), daemon=False)
        consumer = threading.Thread(target=self.consumer, args=(tasks_queue, results_queue,), daemon=False)
        # producer.daemon = False
        # consumer.daemon = False
        producer.start()
        time.sleep(1)  # 保证unfinished_tasks不为0，否则tasks_queue.join()不会阻塞
        consumer.start()
        tasks_queue.join()  # 等待所有队列中的任务都执行完毕，即：tasks_queue.unfinished_tasks==0，否则阻塞
        producer.join()  # 主线程等待producer结束才能结束
        consumer.join()  # 主线程等待consumer结束才能结束


def synchronize():
    """
    同步机制：
        Semaphore
        Lock(RLock)
        Condition
        Event
        Queue
    """
    # obj = SynchronizeWithSemaphore()
    # obj = SynchronizeWithLock()
    # obj = SynchronizeWithCondition()
    # obj = SynchronizeWithEvent()
    obj = SynchronizeWithQueue()
    obj.start()


def main():
    synchronize()


if __name__ == '__main__':
    main()
