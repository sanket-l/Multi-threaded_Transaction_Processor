import time

class Transaction:

    def __init__(self, txn_id, read_keys, write_keys):
        self.id = txn_id
        self.read_set = read_keys
        self.write_set = write_keys
        self.local_buffer = {}
        self.start_time = time.time()
        self.end_time = None

    def complete(self):
        self.end_time = time.time()

    def response_time(self):
        return self.end_time - self.start_time
