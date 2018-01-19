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
    def __init__(self):
        self.queue = queue.Queue(maxsize=10)

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
            try:
                num = self.queue.get(block=False)
            except queue.Empty:
                print('{}: get num failed'.format(t.name))
            else:
                print('{}: {}'.format(t.name, num))
            time.sleep(random.randint(1, 3))

    def producer(self):
        t = threading.current_thread()
        while True:
            try:
                self.queue.put(random.randint(1, 10), block=False)
                self.queue.put(random.randint(1, 10), block=False)
                self.queue.put(random.randint(1, 10), block=False)
                self.queue.put(random.randint(1, 10), block=False)
            except queue.Full:
                print('{}: queue is full'.format(t.name))
            time.sleep(1)


def main():
    # obj = SynchronizeWithSemaphore()
    # obj = SynchronizeWithLock()
    # obj = SynchronizeWithCondition()
    # obj = SynchronizeWithEvent()
    obj = SynchronizeWithQueue()
    obj.start()


if __name__ == '__main__':
    main()
