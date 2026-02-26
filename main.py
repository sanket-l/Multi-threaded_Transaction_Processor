from config import Config, CCMode
from storage.database import KeyValueDB
from loader.data_loader import DataLoader
from occ.occ_manager import OCCManager
from ccc.lock_manager import LockManager
from ccc.twopl_manager import TwoPLManager
from workload.workload_executor import WorkloadExecutor
from metrics.metrics import Metrics

NUM_THREADS = 4
NUM_TRANSACTIONS = 2000
CONTENTION_PROBABILITY = 0.7
HOTSET_SIZE = 10
KEYSPACE_SIZE = 1000
MODE = "OCC"  # change to TWO_PL for 2PL
WORKLOAD_FILE = "workload1/workload1.txt"
INPUT_FILE = "workload1/input1.txt"

def main():
    config = Config(
        num_threads=NUM_THREADS,
        num_transactions=NUM_TRANSACTIONS,
        contention_probability=CONTENTION_PROBABILITY,
        hotset_size=HOTSET_SIZE,
        mode= CCMode.OCC if MODE == "OCC" else CCMode.TWO_PL
    )

    db = KeyValueDB()
    DataLoader.load_from_file(db, INPUT_FILE)

    # occ_manager = OCCManager(db)
    # lock_manager = LockManager()
    # twopl_manager = TwoPLManager(db, lock_manager)
    # metrics = Metrics()

    # WorkloadExecutor.run(config, db, occ_manager, twopl_manager, metrics)

    # print("Throughput:", metrics.throughput())
    # print("Average Response Time:", metrics.average_response_time())
    # print("Aborts:", metrics.aborts)


if __name__ == "__main__":
    main()
