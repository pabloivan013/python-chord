from utils.update import updateAttr
from utils.response import Response

class GetSetResponse(Response):
    def __init__(self, key=None, payload=None):
        super().__init__(payload=payload)
        self.key = key
        self.node_reached = None
        self.nodes_visited = []

