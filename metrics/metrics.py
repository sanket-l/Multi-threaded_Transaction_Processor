import threading
import time

class Metrics:

    def __init__(self):
        self.commits = 0
        self.aborts = 0
        self.response_times = []
        self.lock = threading.Lock()
        self.start_time = time.time()

    def record_commit(self, txn):
        with self.lock:
            self.commits += 1
            self.response_times.append(txn.response_time())

    def record_abort(self):
        with self.lock:
            self.aborts += 1

    def throughput(self):
        duration = time.time() - self.start_time
        return self.commits / duration if duration > 0 else 0

    def average_response_time(self):
        if not self.response_times:
            return 0
        return sum(self.response_times) / len(self.response_times)
