from utils.update import updateAttr
from utils.message import Message

class RequestMessage(Message):

    def __init__(self, type=None, key=None, origen=None, destination=None, payload=None):
        super().__init__(origen, destination, payload)
        self.type = type
        self.key = key
        