from utils.update import updateAttr
class Message:
    
    def __init__(self, origen=None, destination=None, payload=None):
        super().__init__()
        self.origen = origen
        self.destination = destination
        self.payload = payload
        

    def update(self, dict):
        updateAttr(self, dict)