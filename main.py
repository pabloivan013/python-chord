from chord.client import Client
from chord.node import Node
import sys, enum, hashlib

class Command(str, enum.Enum):
    GET     = 'GET'
    SET     = 'SET'
    JOIN    = 'JOIN'
    CREATE  = 'CREATE'

class Flag(str, enum.Enum):
    NODEHOST  = "-nh"
    NODEPORT  = "-np"
    PORT      = "-p"
    KEY       = "-k"
    VALUE     = "-v"
    HELP      = "-h"

def hash_val(value):
    myhash = hashlib.sha1(value.encode())
    return int(myhash.hexdigest(), 16)

def createUsage():
    return ("\nCOMMAND: CREATE [OPTIONS] \n" +
            " Starts a new chord ring \n" +
            " OPTIONS: \n" +
            "   [-p] <PORT> The port where you will listening. Default 5000 \n"
    )

def joinUsage():
    return ("\nCOMMAND: JOIN [OPTIONS] \n" +
            " Joins to a chord node \n" +
            " OPTIONS: \n" +
            "  -nh <NODEHOST>   The node host to join \n" +
            "  -np <NODEPORT>   The node port to join \n" +
            "  [-p] <PORT>      The port where you will listening. Default 5000 \n"
           )

def getUsage():
    return ("\nCOMMAND: GET [OPTIONS] \n" +
            " Search the value of a given key in a chord node \n" +
            " OPTIONS: \n" +
            "  -nh <NODEHOST> The node host to perform the search \n" +
            "  -np <NODEPORT> The node port to perform the search \n" +
            "  -k <KEY>       The key to search \n"
    )

def setUsage():
    return ("\nCOMMAND: SET [OPTIONS] \n" +
            " Sets a key value pair in a specific node \n" +
            " OPTIONS: \n" +
            "  -nh <NODEHOST> The node host to set the key value \n" +
            "  -np <NODEPORT> The node port to set the key value \n" +
            "  -k <KEY>       The key to set \n" +
            "  -v <VALUE>     The value to set \n" 
    )

def usage():
    print("USAGE: <COMMAND> [OPTIONS] \n" +
            createUsage(),
            joinUsage(),
            getUsage(),
            setUsage()
        )

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
                print(joinUsage())
        elif command == Command.GET:
            node = create_node(node_host, node_port)
            if node and key:
                response = client.get(key, node)
                if response:
                    print("GET RESP: ", response)
            else:
                print(getUsage())
        elif command == Command.SET:
            node = create_node(node_host, node_port)
            if node and key != None and value:
                data = {'key': key,'value': value}
                response = client.set(data, node)
                if response:
                    print("SET RESP: ", response)
            else:
                print(setUsage())
        else:
            print(f"COMMAND: {command} NOT FOUND \n")
            usage()
    
    except Exception as e :
        print("ERROR MAIN: ", e)
    

if __name__ == '__main__':
    main()
    