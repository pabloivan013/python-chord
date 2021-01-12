import socket, select, threading, time, sys, logging
from socket import SHUT_RDWR
from chord.logger import logger
from utils.response import Response


class TCPClientServer:
    
    MAX_CONNECTIONS = 100
    RESPONSE_TIMEOUT = 3

    def __init__(self, host, port, node):
        super().__init__()
        self.host = host
        self.port = port
        self.node = node
        self.server_sock = None
        self.connections = []
        self.signal_thread = True

    def init_server(self):
        """
        Performs the server initialization
        :return: A Response object with the status of the server
        """
        try:
            server_response = Response()
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.bind((self.host, self.port))
            self.server_sock.listen(self.MAX_CONNECTIONS)
            self.connections.append(self.server_sock)
            server_response.success = True
            server_response.payload = f"Server started at ({self.host}:{self.port})"
            return server_response
        except Exception as e:
            server_response.success = False
            server_response.error = f"ERROR init_server: {e}"
            return server_response

        
    def _handle_data(self, data, sock):
        """ 
        Handles the message requests sent by other nodes
        :param data: The message sended by other node
        :return: A Response object in json format containing the result of the operation requested
        """
        try:
            message = data
            logger.info(f"Message: {message}  -  From: {sock.getpeername()}")
            response = self.node.handle_message(message)
            sock.send(response.encode('utf-8'))
        except Exception as e:
            logger.exception(f" ERROR SERVER ID: {self.node.id} _handle_data")
            sock.close()
        

    # sock.setblocking(True) is equivalent to sock.settimeout(None)
    # sock.setblocking(False) is equivalent to sock.settimeout(0.0)
    def _recv_alldata(self, sock, timeout, end_time):
        """
        Receives all the data sended by other servers node
        :return: A Response object containing the data received
        """
        try:
            data_response = Response()
            fragments = []
            max_buff = 1024
            #sock.setblocking(True)
            sock.settimeout(timeout)
            while True: 
                chunk = sock.recv(max_buff).decode('utf-8')
                fragments.append(chunk)
                #sock.setblocking(False)
                sock.settimeout(end_time)
                if not chunk: #len(chunk) < max_buff:
                    break
            
        except socket.timeout as e:
            logger.exception(f" ERROR SERVER ID: {self.node.id} TIMEOUT _recv_alldata")
            data_response.success = False
            data_response.error = f"ERROR _recv_alldata TIMEOUT: {e}"
            return data_response
        except socket.error as e:
            # 10035: Windows
            # 11: Linux
            # Code 10035 or 11: recv has no data available to read, the message has ended
            errno = e.errno
            # EAGAIN or EWOULDBLOCK codes
            if errno != 10035 and errno != 11:
                logger.error(f" ERROR SERVER ID: {self.node.id} _recv_alldata: {e}")
                data_response.success = False
                data_response.error = f"ERROR _recv_alldata: {e}"
                return data_response
        
        data = ''.join(fragments)
        data_response.payload = data
        data_response.success = True
        return data_response   

    def _handle_requests(self):
        """ 
        Manages the connections with others servers and handle the data received
        Runs a thread per every request sended by other server node
        """
        while self.signal_thread:
            
            read_sockets,write_sockets,error_sockets = select.select(self.connections,[],[])

            for sock in read_sockets:
                try:
                    if sock == self.server_sock:
                        sockfd, address = self.server_sock.accept()
                        self.connections.append(sockfd)
                        logger.info(f"Client connected {address} on {self.host}:{self.port}")
                    else:
                        data_response = self._recv_alldata(sock, None, 0.0)
                        if data_response.success and data_response.payload:
                            threading.Thread(target=self._handle_data, args=(data_response.payload, sock, )).start()
                        else:
                            if sock in self.connections:
                                self.connections.remove(sock)
                            sock.close()
                except Exception as e:
                    logger.exception(f" ERROR SERVER ID: {self.node.id} _handle_requests")
                    sock.close()
                    if sock in self.connections:
                        self.connections.remove(sock)

    def start_server(self):
        """ 
        Initializes and starts the server
        :return: A Response object with the server status
        """
        server_status = self.init_server()
        if server_status.success:
            self.signal_thread = True
            threading.Thread(target=self._handle_requests).start()
        return server_status

    def stop_server(self):
        """
        Stops the server and clear connections
        """
        try:
            print(f"stop server 0: {self.node.id}")
            self.signal_thread = False
            self.connections.clear()
            if self.server_sock:
                #self.server_sock.close()
                self.server_sock.shutdown(SHUT_RDWR)
                
                print(f"stop server 1: {self.node.id}")
        except Exception as e:
            logger.exception(f" STOP SERVER ID: {self.node.id} stop_server, error: {e}")
        finally:
            if self.server_sock:
                self.server_sock.close()
        
    def send_message(self, host, port, message):
        """ 
        Establish a tcp connection with other server node and tries to sends a message 
        :param host: The server node host
        :param port: The server node port
        :param message: The String message to be send
        :return: A Response object with the result of the operation sended by the node contacted
        """
        try:
            server_response = Response()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.sendall(message.encode('utf-8'))
            server_response = self._recv_alldata(sock, self.RESPONSE_TIMEOUT, 0.0)
            return server_response
        except Exception as e:
            logger.exception(f" ERROR SERVER ID: {self.node.id} send_message")
            server_response.success = False
            server_response.error = f"ERROR send_message: {e}"
            return server_response
        finally:
            sock.close()
            