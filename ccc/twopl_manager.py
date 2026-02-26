import time
import random

class TwoPLManager:

    def __init__(self, db, lock_manager):
        self.db = db
        self.lock_manager = lock_manager

    def execute(self, txn):

        all_keys = list(set(txn.read_set + txn.write_set))

        while not self.lock_manager.try_acquire_all(all_keys):
            time.sleep(random.uniform(0.001, 0.005))  # livelock prevention

        # Execute
        for key in txn.read_set:
            txn.local_buffer[key]["value"] = self.db.get(key)

        for key in txn.write_set:
            txn.local_buffer[key]["value"] += 1

        for key in txn.write_set:
            self.db.put(key, txn.local_buffer[key])

        self.lock_manager.release_all(all_keys)

        return True
