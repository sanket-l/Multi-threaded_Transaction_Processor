import threading
import random
from transaction.transaction import Transaction
from config import CCMode
from ccc.lock_manager import LockManager

class WorkloadExecutor:

    @staticmethod
    def run(config, db, occ_manager, twopl_manager, metrics):

        def worker(thread_id):

            for i in range(config.num_transactions // config.num_threads):

                keys = WorkloadExecutor.select_keys(config)

                txn = Transaction(
                    txn_id=f"{thread_id}-{i}",
                    read_keys=[keys[0]],
                    write_keys=[keys[1]]
                )

                while True:
                    if config.mode == CCMode.OCC:
                        success = occ_manager.execute(txn)
                    else:
                        success = twopl_manager.execute(txn)

                    if success:
                        txn.complete()
                        metrics.record_commit(txn)
                        break
                    else:
                        metrics.record_abort()

        threads = []

        for t in range(config.num_threads):
            thread = threading.Thread(target=worker, args=(t,))
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

    @staticmethod
    def select_keys(config):

        if random.random() < config.contention_probability:
            key1 = random.randint(0, config.hotset_size - 1)
            key2 = random.randint(0, config.hotset_size - 1)
        else:
            key1 = random.randint(0, config.keyspace_size - 1)
            key2 = random.randint(0, config.keyspace_size - 1)

        return f"key{key1}", f"key{key2}"
