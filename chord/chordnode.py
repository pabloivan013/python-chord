from chord.node import Node
from chord.type import Type
from chord.logger import logger
from chord.storage import Storage
from chord.fingernode import FingerNode
from chord.tcpclientserver import TCPClientServer
from utils.response import Response
from utils.requestmessage import RequestMessage
from utils.getsetresponse import GetSetResponse


import threading, json, enum, time, logging, sys, ast

class ChordNode(Node):

    STABILIZATION_TIME = 3
    REPLICATION_TIME   = 4
    CLEAR_CACHE_TIME   = 3
    
    def __init__(self, id, host, port, mbits):
        super().__init__(id, host, port)
        self.successor = None
        self.predecessor = None
        self.mbits = mbits
        self.next = -1
        self.finger_table = [FingerNode(id, host, port) for _ in range(mbits)]
        self.own_data = Storage() # Data from (pred.id , self.id]
        self.replicated_data = Storage()   # Data from pred.id
        self.cache = Storage()
        self.server = TCPClientServer(self.host, self.port, self)
        self.server_started = False
        self.sleep_time = 0.5
        
    def start(self):
        """
        Initializes the server and the stabilization threads
        :return: A Response object containing the status of the server
        """
        server_status = self.server.start_server()
        if server_status.success:
            self.server_started = True
            threading.Thread(target=self._run_function_thread, 
                            args=(self.stabilize, self.sleep_time)).start()

            threading.Thread(target=self._run_function_thread, 
                            args=(self.fix_fingers, self.sleep_time)).start()

            threading.Thread(target=self._run_function_thread, 
                            args=(self.check_predecessor, self.sleep_time)).start()
                        
            threading.Thread(target=self._run_function_thread, 
                            args=(self.replication, self.REPLICATION_TIME)).start()

            threading.Thread(target=self._run_function_thread,
                            args=(self.clear_cache, self.CLEAR_CACHE_TIME)).start()
        return server_status

    def stop(self):
        self.server.stop_server()
        self.server_started = False


    def handle_message(self, str_message):
        """ 
        Performs the operations requested by others nodes
        :param str_message: A RequestMessage object in json format containing the requested operation
        :return: A Response object in json format containing the result of the operation requested
        """
        try:
            message = RequestMessage()
            message.update(json.loads(str_message))
            response = Response(origen=self.id, destination=message.origen)
            
            request_type = message.type
            key  = message.key
            data = message.payload

            if (not request_type):
                raise ValueError('Type not provided')

            if (request_type == Type.FIND_SUCCESSOR):
                successor_k = self.find_successor(key)
                if (not successor_k):
                    raise ValueError('FIND_SUCCESSOR: Successor not found')
                response.payload = vars(successor_k)

            elif (request_type == Type.GET_SUCCESSOR):
                response.payload = vars(self.successor)

            elif (request_type == Type.GET_DATA):
                logger.info("enter get_search")
                data = self.get(key)
                if not data:
                    raise ValueError('GET_DATA: data not found')
                response.payload = data

                # if (data):
                #     logger.info(f"DATA handle_message id: {self.id} - {data}")
                #     response['success'] = True
                #     response.payload = data

            elif (request_type == Type.GET_PREDECESSOR):
                logger.info("enter get_predecessor")
                response.payload = vars(self.predecessor) if self.predecessor else None

            elif (request_type == Type.NOTIFY):
                logger.info("Enter notify")
                self.notify(data)

            elif (request_type == Type.CHECK_STATUS):
                response.payload = {"status":"ok", "server_id": self.id}
                
            elif (request_type == Type.SET_DATA):
                set_data = self._set_data(data)
                response.payload = set_data

            elif (request_type == Type.GET_KEYS):
                keys = self._get_keys(data)
                response.payload = str(keys)

            elif (request_type == Type.REPLICATION):
                self._save_replicated_data(data, message.origen)
                
            else:
                raise TypeError(f"Type {Type} not found")

            response.success = True
            return json.dumps(vars(response))
        except Exception as e:
            logger.exception(f"Error handle_message: {e}")
            response.success = False
            response.error = "Error handle_message exception"
            return json.dumps(vars(response))

    def _save_replicated_data(self, data, pred_id):
        """
        Saves the data sended by other node in the replicated_data storage
        The node who send the information has to be his predecessor
        :param data: The data to save
        :param pred_id: The node id sending the data
        """
        if self.predecessor and self.predecessor.id == pred_id:
            self.replicated_data.set_store_data(ast.literal_eval(data))


    def _handle_server_response(self, server_response):
        """
        Process the server response originated by a request message
        :param server_response: The response from the server
        :return: The payload of the server response if the request has been successful
        """
        try:
            if server_response.success and server_response.payload:
                request_response = Response()
                request_response.update(json.loads(server_response.payload))
                
                if request_response.success:
                    return request_response.payload
                else:
                    print(f"ERROR request_response: {request_response.error}")
            else:
                print(f"ERROR server_response: {server_response.error}")
        except Exception as e:
            logger.exception(f"ERROR _handle_server_response: {repr(e)}")

 
    def send_request(self, type, key, node, data = None):
        """ 
        Sends a request message containing a operation to the node address
        :param type: The type of operation to perform
        :param key: The key used to perform a search operation
        :param node: The node to send the operation
        :param data: Data sended to the node
        :return: The result of the operation performed by the node, or None in case of error.
        """
        try:
            message = RequestMessage(type, key, self.id, node.id, data)
            message = json.dumps(vars(message))
            server_response = self.server.send_message(node.host, node.port, message)
            return self._handle_server_response(server_response)
        except Exception as e:
            logger.exception("ERROR send_request: ", repr(e))
        
    
    def _get_keys_in_interval(self, start, end, data_from, data_to, ignore_key = False):
        """ 
        Obtains all the keys in the range of (start, end] from 'data_from' 
        and passests it to 'data_to' if 'ignore_key' its false

        :param start: Start of the key range
        :param end: End of the key range
        :param data_from: The data structure from where the keys are obtained
        :param data_to: The data structure from where the keys are stored
        :param ignore_key: A boolean used to ignore the key if its in 'data_from'
        :return: A dict object containing the keys requested
        """
        try:
            response_data = {}
            for key,value in data_from.copy().items():
                # data_to already have the key
                if ignore_key and data_to.get(key, None):
                    continue 

                if (self.interval(start, key, end, True)):
                    data_k = data_from.pop(key)
                    response_data[key] = data_k
                    data_to[key] = data_k 
            return response_data
        except Exception as e:
            print("ERROR _get_keys_in_interval: ", e)
        

    def _get_keys_only_nodes(self, start, end, max_nodes):
        response_data = {}
        i = start 
        while True:
            data_k = self.search(i)  
            print(f"node: {self.id} - i: {i}, data: {data_k}")
            if data_k:
                response_data[i] = data_k
            if i == end:
                break
            i = (i + 1) % max_nodes
        return response_data

    def _nodes_between(self, start, end, max_nodes):
        if start <= end:
            return (end - start) + 1
        else:
            return ((max_nodes - start) + end) + 1


    def _get_keys(self, keys):
        """ 
        Obtains the keys in a range requested by other node when joins the chord network
        :param keys: A dict object containing the range of the requested keys
        :return: A dict object with the keys requested
        """
        max_nodes = 2**self.mbits
        key_start = keys['start'] % max_nodes
        key_end   = keys['end'] % max_nodes
       
        number_dict_data = len(self.own_data.get_store())
        number_beetween_nodes = self._nodes_between(key_start, key_end, max_nodes)
        
        return self._get_keys_in_interval((key_start-1) % max_nodes, key_end,
                                            self.own_data.get_store(), self.replicated_data.get_store())

        # if number_dict_data < number_beetween_nodes:
        #     return self._get_keys_in_interval((key_start-1) % max_nodes, key_end,
        #                                         self.own_data, self.replicated_data)
        # else:
        #     return self._get_keys_only_nodes(key_start, key_end, max_nodes)
        
        
    def search(self, k):
        """ 
        Search for the value of a key in the cache and then in the own_data storage
        :param k: The key to search
        :return: The value of the key or None if not exists
        """
        data = self.cache.get_key(k)
        return data if data else self.own_data.get_key(k)
    

    def interval(self, a, k, b, equal):
        """ 
        Verifies that 'k' is in the interval (a,b) or (a,b] if equal its true
        :param a: Start of the interval
        :param b: End of the interval
        :param equal: Boolean used to check if 'k' is in (a,b]
        :return: True if 'k' is in (a,b) / (a,b], False otherwise
        """
        max_nodes = self.mbits**2
        a = a % max_nodes
        k = k % max_nodes
        b = b % max_nodes

        if (a < b):
            return (a < k and (k <= b if equal else k < b)) 
        else: # a > b
            return not ( k <= a and (k > b if equal else k >= b))

    def _set_data(self, data):
        """
        Set a (key,value) in the node data storage only if the node id is the successor of the key
        otherwise searches for the successor of the key and send a request message with the data to be saved
        :param data: A dict object containing the key and value
        :return: A GetSetResponse object with the status of the operation
        """
        try:
            key   = data['key'] % 2**self.mbits
            value = data['value']
            set_response = GetSetResponse(key, value)
            print(f"data: {data}")
            
            if self.predecessor and self.interval(self.predecessor.id, key, self.id, True):
                # data key in (pred, n]
                self.own_data.set_key_value(key, value)
                set_response.success = True
                set_response.node_reached = self.id
            else:
                successor = self.find_successor(key)
                if successor:
                    request_response = self.send_request(Type.SET_DATA, key, successor, data)
                    if request_response:
                        set_response.update(request_response)
                else:
                    set_response.success = False
                    set_response.error = "Successor not found"
            
            node = Node(self.id, self.host, self.port)
            set_response.nodes_visited.insert(0, vars(node))
            return vars(set_response)
        except Exception as e:
            logger.exception(f"ERROR _set_data")
            set_response.success = False
            set_response.error = "Error _set_data exception"
            return vars(set_response)

    def get(self, k):
        """ 
        Searches for the value of a key in the node data storages 
        If the key does not exists and the current node id it's not his successor 
        sends a request message to the successor node of the key asking for his value
        :param k: The key to search
        :return: A GetSetResponse object with the status of the operation 
        """
        get_response = GetSetResponse(k)
        data = self.search(k)
        if (data or self.id == k or (self.predecessor and self.interval(self.predecessor.id, k, self.id, True))):
            get_response.payload = data
            get_response.node_reached = self.id
            get_response.success = True
        else:
            successor_k = self.find_successor(k)
            if successor_k:       
                request_response = self.send_request(Type.GET_DATA, k, successor_k)
                if request_response:
                    get_response.update(request_response)
                    if get_response.success and get_response.payload:
                        self.cache.set_key_value(get_response.key, get_response.payload)
                    
            else:
                get_response.success = False
                get_response.error = 'Successor not found'

        node = Node(self.id, self.host, self.port)
        get_response.nodes_visited.insert(0, vars(node))
        return vars(get_response)


    def find_successor(self, k):
        """ 
        Searches for the successor of a key
        :param k: The key used in the search
        :return: A Node object representing the successor of the key, None otherwise
        """
        try:
            if(self.interval(self.id, k, self.successor.id, True)):
                return self.successor
            else:
                cpn = self.closest_precedent_node(k)
                successor_k = self.send_request(Type.FIND_SUCCESSOR, k, cpn)
                if successor_k:
                    successor_k = Node(successor_k['id'], successor_k['host'], successor_k['port'])  
                return successor_k   
        except Exception as e:
            logger.exception(" ERROR find_successor")
                                
        
    def closest_precedent_node(self, k):
        for i in range(self.mbits-1, -1, -1):
            if (self.finger_table[i] and self.interval(self.id, self.finger_table[i].id, k, False)): #Finger table not empty
                return self.finger_table[i]
                        
        return self


    def create(self):
        self.predecessor = None
        self.successor = self.finger_table[0]


    def _ask_keys(self):
        """ 
        Tries to get the keys corresponding to the node when it joins to a chord network 
        Sends a request message to the successor node with his predecessor node id and his own id
        :return: True if the request has been successful, False otherwise
        """
        MAX_TRY = 50
        for i in range(MAX_TRY):
            print(f"Node :{self.id} - Pred: {self.predecessor.id if self.predecessor else None} Asking for keys to successor node: {self.successor.id} - Try: {i+1}/{MAX_TRY}")
            if (self.predecessor and (self.predecessor.id != self.id)):
                keys = self.send_request(Type.GET_KEYS, self.id, self.successor,
                                        {"start": self.predecessor.id + 1, "end": self.id})
                # Clear keys if they don't belong to the node
                for key,value in self.own_data.get_store().copy().items():
                    if (not self.interval(self.predecessor.id, key, self.id, True)):
                        data_k = self.own_data.get_store().pop(key)
                        
                self.own_data.update_store_data(ast.literal_eval(keys))
                return True    
            time.sleep(self.sleep_time)
        return False

    
    def join(self, p):
        """ 
        Sends a request message to a node asking for his successor and proceed to update it
        Performs the search of the keys that belong to the node
        :param p: The node to send the request
        :return: True if response of the node and keys was successful, False otherwise
        """
        try:
            self.predecessor = None
            success = False
            response = self.send_request(Type.FIND_SUCCESSOR, self.id, p)
            logger.info(f"Response: {response}")
            if response:
                self.successor.update(response)
                success = self._ask_keys()
            return success
        except Exception as e:
            logger.exception(f" ERROR join")
        
    #
    def stabilize(self):
        """ 
        Verifies if his successor node has changed and proceed to update it
        Sends a notify message to his successor node for update his predecessor with this node
        """
        x = self.send_request(Type.GET_PREDECESSOR, self.successor.id, self.successor)

        if ( x and self.interval(self.id, x['id'], self.successor.id, False)):
            self.successor.update(x)

        # Successor has failed, replace with the first live entry in its successor list
        # Or predecessor its None...
        if (not x):
            self.successor.update(vars(self))

        self.send_request(Type.NOTIFY, self.id, self.successor,
                        {"id": self.id, "host": self.host, "port": self.port})


    def notify(self, n):
        """ 
        Performs a predecessor update if the id of the node sending the notification 
        continues after the actual predecessor id
        Before the update, verifies whose keys in replicated data should be stored in own data
        :param n: The node who sends the notification
        """
        if (n and self.predecessor == None or self.interval(self.predecessor.id, n['id'], self.id, False)):
            # Predecessor update, first move replicated_data to own_data
            self._get_keys_in_interval(n['id'], self.id, self.replicated_data.get_store(), self.own_data.get_store(), True)
        
            # Update predecessor
            self.predecessor = Node(n['id'], n['host'], n['port'])
                
    #
    def fix_fingers(self):
        """ 
        Updates the fingers nodes with his actuals successors
        """
        self.next = (self.next + 1) % self.mbits
        level = (self.id + 2**self.next) % 2**self.mbits
        finger = self.find_successor(level)
        if finger:
            self.finger_table[self.next].update(vars(finger))
            self.finger_table[self.next].level = level
            
    
    def print_fingers(self):
        for i, f in enumerate(self.finger_table):
            print(i, vars(f) if f else None)


    def check_predecessor(self):
        """
        Verifies that the predecessor nodo has not failed, sets it to None if its has fail
        """
        if self.predecessor:
            predecessor_status = self.send_request(Type.CHECK_STATUS, self.id, self.predecessor)                                  
            if(not predecessor_status):
                self.predecessor = None

    def replication(self):
        """ 
        Sends a message containing the data from the node to his successor
        """
        if self.successor.id != self.id:
            self.send_request(Type.REPLICATION, self.id, self.successor, str(self.own_data.get_store()))

    def clear_cache(self):
        self.cache.clear_store()

    def _run_function_thread(self, func, sleep_time):
        """ 
        Runs a function passed by parameter every certain time while the server is running
        :param func: The function to be executed
        :param sleep_time: The time between the function invocation
        """
        while self.server_started:
            try:
                func()
                time.sleep(sleep_time)
            except Exception as e:
                logger.exception(f" ERROR _run_function_thread")
            