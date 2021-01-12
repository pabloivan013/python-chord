from chord.client import Client
from chord.node import Node
import sys, enum, hashlib

class Command(str, enum.Enum):
    GET     = 'GET'
    SET     = 'SET'
    JOIN    = 'JOIN'
    CREATE  = 'CREATE'


def hash_val(value):
    myhash = hashlib.sha1(value.encode())
    return int(myhash.hexdigest(), 16)

def usage():
    print("USAGE: <COMMAND> -FLAGS")

def create_node(host, port):
    if host and port:
        node_address = host +':'+ str(port)
        node_id = hash_val(node_address)
        return Node(node_id, host, port)

def main():

    client      = None
    client_id   = None
    client_host = '127.0.0.1' # Default ip
    client_port = 5000        # Default port

    key         = None
    value       = None
    node_id     = None
    node_host   = None
    node_port   = None
    command     = None

    print ('Number of arguments:', len(sys.argv), 'arguments.')
    argv = sys.argv
    argn = len(sys.argv) 

    if argn < 2 :
        usage()
        return

    command = argv[1]

    i = 2
    while i < argn:
        if (argv[i] == "-p" and (argn-i) >= 2):
            i += 1
            client_port = int(argv[i])
        elif (argv[i] == "-nh" and (argn-i) >= 2):
            i += 1
            node_host = str(argv[i])
        elif (argv[i] == "-np" and (argn-i) >= 2):
            i += 1
            node_port = int(argv[i])
        elif (argv[i] == "-k" and (argn-i) >= 2):
            i += 1
            key = hash_val(str(argv[i])) # SHA
        elif (argv[i] == "-v" and (argn-i) >= 2):
            i += 1
            value = str(argv[i])
        else:
            pass
        i += 1
        
    # print(  f"key = {key} \n",
    #         f"value = {None} \n",
    #         f"client = {client} \n",
    #         f"client_host = {client_host} \n",
    #         f"client_port= {client_port} \n",
    #         f"node_host = {node_host} \n",
    #         f"node_port = {node_port} \n",
    #         f"command = {command}")
    
    
    try:
        client_address = client_host +':'+ str(client_port)
        client_id = hash_val(client_address)
        client = Client(client_id, client_host, client_port)

        if command == Command.CREATE:
            client.start()
        elif command == Command.JOIN:
            node = create_node(node_host, node_port)
            if node:
                client.join(node)
            else:
                print("JOIN USAGE")
        elif command == Command.GET:
            node = create_node(node_host, node_port)
            if node and key:
                response = client.get(key, node)
                if response:
                    print("GET RESP: ", response)
            else:
                print("GET USAGE")
        elif command == Command.SET:
            node = create_node(node_host, node_port)
            if node and key != None and value:
                data = {'key': key,'value': value}
                response = client.set(data, node)
                if response:
                    print("SET RESP: ", response)
            else:
                print("SET USAGE")
        else:
            print(f"Command: {command} not found")
    except Exception as e :
        print("ERROR MAIN: ", e)
    


if __name__ == '__main__':
    main()
    