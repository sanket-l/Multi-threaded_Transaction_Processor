from rockstore import RockStore
import threading
import json
import os


class KeyValueDB:

    def __init__(self, db_path="kvstore.db"):
        if os.path.exists(db_path):
            import shutil
            shutil.rmtree(db_path)

        options = {
            'create_if_missing': True,
            'compression_type': 'lz4_compression',
            'write_buffer_size': 64 * 1024 * 1024,  # 64MB
            'max_open_files': 300000
        }

        self.db = RockStore(db_path, options)
        self.lock = threading.Lock()   # thread-safety for Python

    def get(self, key):
        with self.lock:
            value = self.db.get(key.encode())
            if value is None:
                return None
            return json.loads(value.decode())

    def put(self, key, value):
        with self.lock:
            self.db.put(
                key.encode(),
                json.dumps(value).encode()
            )

    def delete(self, key):
        with self.lock:
            self.db.delete(key.encode())
