import threading

class Storage:
    
    def __init__(self):
        super().__init__()
        self.store = dict()
        self.lock = threading.Lock()

    def get_key(self, key):
        with self.lock:
            return self.store.get(key, None)

    def set_key_value(self, key, value):
        with self.lock:
            self.store[key] = value
    
    def get_store(self):
        with self.lock:
            return self.store

    def set_store_data(self, data):
        with self.lock:
            self.store = data
    
    def update_store_data(self, data):
        with self.lock:
            self.store.update(data)
    
    def clear_store(self):
        with self.lock:
            self.store.clear()