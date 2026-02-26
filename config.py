from enum import Enum

class CCMode(Enum):
    OCC = 1
    TWO_PL = 2

class Config:
    def __init__(self,
                 num_threads=4,
                 num_transactions=1000,
                 contention_probability=0.5,
                 hotset_size=10,
                 keyspace_size=1000,
                 mode=CCMode.OCC):

        self.num_threads = num_threads
        self.num_transactions = num_transactions
        self.contention_probability = contention_probability
        self.hotset_size = hotset_size
        self.keyspace_size = keyspace_size
        self.mode = mode
