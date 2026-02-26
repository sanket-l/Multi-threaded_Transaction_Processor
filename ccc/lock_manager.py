import threading

class LockManager:

    def __init__(self):
        self.locks = {}
        self.global_lock = threading.Lock()

    def try_acquire_all(self, keys):

        with self.global_lock:
            for key in keys:
                if self.locks.get(key, False):
                    return False

            for key in keys:
                self.locks[key] = True

            return True

    def release_all(self, keys):
        with self.global_lock:
            for key in keys:
                self.locks[key] = False
