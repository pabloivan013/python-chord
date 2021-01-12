from chord.chordnode import ChordNode
from chord.chordnode import Type
import time

class Client:
    
    GET_SET_DATA_TRYS = 3
    GET_SET_WAIT_TIME = 1
    MBITS = 160

    def __init__(self, id, host, port):
        super().__init__()
        self.chord_client = ChordNode(id, host, port, self.MBITS)
        self.node_joined = None

    def start(self):
        self.chord_client.create()
        if not self.chord_client.server_started:
            server_status = self.chord_client.start()
            if server_status.success:
                print(server_status.payload)
            else:
                print(server_status.error)
                return False
        return True

    def stop(self):
        if self.chord_client.server_started:
            self.chord_client.stop()

    def join(self, node):
        self.node_joined = None
        if self.start():
            if self.chord_client.join(node):
                print(f"Connected to {node.host}:{node.port}")
                self.node_joined = node
            else:
                print(f"Fail to join node {node.host}:{node.port}")
                self.stop()
            
    def leave(self):
        if self.server_started:
            self.chord_client.stop()

    # client consult node n for key k
    def get(self, key, node):
        return self.get_set_data(Type.GET_DATA, node, key=key)
    
    # client set data (key:value)
    def set(self, data, node):
        return self.get_set_data(Type.SET_DATA, node, data=data)
        

    def get_set_data(self, type, node, key=None, data=None):
        trys = self.GET_SET_DATA_TRYS
        while trys > 0:
            get_response = self.chord_client.send_request(type, key, node, data)
            if get_response:
                return get_response
            time.sleep(self.GET_SET_WAIT_TIME)
            trys = trys - 1
        