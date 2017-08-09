import multiprocessing as mp
import time
from multiprocessing import Queue
from queue import Full

import numpy as np


class Worker(mp.Process):
    def __init__(self, worker_id, queue: Queue, timeout):
        super().__init__()
        self.exit = mp.Event()
        self.worker_id = worker_id
        self.queue = queue
        self.timeout = timeout

    def run(self):
        generated = 0
        while not self.exit.is_set():
            while not self.exit.is_set():
                try:
                    self.queue.put(np.random.normal(size=(2, 2)), timeout=self.timeout)
                    generated += 1
                    break
                except Full:
                    pass
        print("Clean exit; Worker #%s has generated %s chunks of data" % (self.worker_id, generated))

    def shutdown(self):
        self.exit.set()


class DistributedGenerator:
    def __init__(self, pool_size=4, max_size=10, timeout=10):
        self.queue = mp.Queue(maxsize=max_size)
        self.pool = [Worker(i, self.queue, timeout) for i in range(pool_size)]
        for p in self.pool:
            p.start()

    def generate(self):
        while True:
            yield self.queue.get()

    def finish(self):
        for p in self.pool:
            p.shutdown()

        for p in self.pool:
            p.join()


if __name__ == '__main__':
    dg = DistributedGenerator(timeout=1)
    n = dg.generate()

    for i in range(10):
        print("Received: %s" % next(n))
        time.sleep(0.5)

    print("Finished. Collecting stats:")
    dg.finish()
