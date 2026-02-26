import threading
import time

class OCCManager:

    def __init__(self, db):
        self.db = db
        self.validation_lock = threading.Lock()
        self.committed_txns = []
        self.timestamp = 0
        self.ts_lock = threading.Lock()

    def execute(self, txn):

        # Assign start timestamp
        with self.ts_lock:
            txn.start_ts = self.timestamp
            self.timestamp += 1

        # READ PHASE
        for key in txn.read_set:
            txn.local_buffer[key]["value"] = self.db.get(key)

        # Simulate logic (increment values)
        for key in txn.write_set:
            txn.local_buffer[key]["value"] += 1

        # VALIDATION PHASE (sequential)
        with self.validation_lock:
            if not self.validate(txn):
                return False

            # WRITE PHASE
            for key in txn.write_set:
                self.db.put(key, txn.local_buffer[key])

            txn.commit_ts = self.timestamp
            self.timestamp += 1
            self.committed_txns.append(txn)

        return True

    def validate(self, txn):
        for committed in self.committed_txns:
            if committed.commit_ts > txn.start_ts:
                if set(committed.write_set) & set(txn.read_set):
                    return False
        return True
