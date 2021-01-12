from utils.update import updateAttr

class Node:
    def __init__(self, id, host, port):
        super().__init__()
        self.id = id
        self.host = host
        self.port = port
        
    def update(self, dict):
        updateAttr(self, dict)