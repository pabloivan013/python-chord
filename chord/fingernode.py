from chord.node import Node

class FingerNode(Node):
    
    def __init__(self, id, host, port, level = -1):
        super().__init__(id, host, port)
        self.level = level
