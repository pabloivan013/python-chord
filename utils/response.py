from utils.update import updateAttr
from utils.message import Message

class Response(Message):
    def __init__(self, origen=None, destination=None, payload=None):
        super().__init__(origen, destination, payload)
        self.success = False
        self.error   = ''
       