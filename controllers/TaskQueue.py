from threading import Thread
from queue import Queue


class TaskQueue(Queue):

    def __init__(self, workers=1):
        """
        initialize a new Queue
        :param workers: number of workers
        """
        Queue.__init__(self)
        self.workers_count = workers
        self.workers = []

    def add_task(self, task, *args, **kwargs):
        """
        add a new task to the queue
        :param task: callable task
        :param args: task args
        :param kwargs: task kwargs
        :return: True on success
        """

        print(self.qsize())

        self.put((task, args or (), kwargs or {}))
        return True

    def start_workers(self):
        """
        start workers in the background
        :return: number of workers
        """
        for _ in range(self.workers_count):
            worker = Thread(target=self.worker)
            self.workers.append(worker)
            worker.start()

        return len(self.workers)

    def worker(self):
        """
        worker's task
        """
        while True:
            task, args, kwargs = self.get(block=True)
            task(*args, **kwargs)
            self.task_done()

