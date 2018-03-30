import queue


class Task(object):
    taskid = 0

    def __init__(self, target):
        Task.taskid += 1
        self.tid = Task.taskid  # Task ID
        self.target = target  # Target coroutine
        self.sendval = None  # Value to send

    def run(self):
        return self.target.send(self.sendval)


class Scheduler(object):
    def __init__(self):
        self.ready = queue.Queue()
        self.taskmap = {}

    def new(self, target):
        newtask = Task(target)
        self.taskmap[newtask.tid] = newtask
        self.schedule(newtask)
        return newtask.tid

    def schedule(self, task):
        self.ready.put(task)

    def mainloop(self):
        while self.taskmap:
            task = self.ready.get()
            try:
                result = task.run()
            except StopIteration:
                self.drop_task(task)
            else:
                self.schedule(task)

    def drop_task(self, task):
        print('task {} done, drop ot from task_map'.format(task.tid))
        del self.taskmap[task.tid]


def foo():
    for _ in range(10):
        print("I'm foo")
        yield


def bar():
    for _ in range(5):
        print("I'm bar")
        yield


def main():
    sched = Scheduler()
    sched.new(foo())
    sched.new(bar())
    sched.mainloop()


if __name__ == '__main__':
    main()
