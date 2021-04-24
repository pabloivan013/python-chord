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
    return (f"\nCOMMAND: {Command.CREATE} [OPTIONS] \n" +
            " Starts a new chord network \n" +
            " OPTIONS: \n" +
           f"   [{Flag.PORT}] <PORT> The port where you will listening. Default 5000 \n"
    )

def joinUsage():
    return (f"\nCOMMAND: {Command.JOIN} [OPTIONS] \n" +
            " Joins to a chord node \n" +
            " OPTIONS: \n" +
           f"  {Flag.NODEHOST}  <NODEHOST>   The node host to join \n" +
           f"  {Flag.NODEPORT}  <NODEPORT>   The node port to join \n" +
           f"  [{Flag.PORT}] <PORT>       The port where you will listening. Default 5000 \n"
           )

def getUsage():
    return (f"\nCOMMAND: {Command.GET} [OPTIONS] \n" +
            " Search the value of a given key in a chord node \n" +
            " OPTIONS: \n" +
           f" {Flag.NODEHOST} <NODEHOST> The node host to perform the search \n" +
           f" {Flag.NODEPORT} <NODEPORT> The node port to perform the search \n" +
           f" {Flag.KEY}  <KEY>      The key to search \n"
           )

def setUsage():
    return (f"\nCOMMAND: {Command.SET} [OPTIONS] \n" +
            " Sets a key value pair in a specific node \n" +
            " OPTIONS: \n" +
            f"  {Flag.NODEHOST} <NODEHOST> The node host to set the key value \n" +
            f"  {Flag.NODEPORT} <NODEPORT> The node port to set the key value \n" +
            f"  {Flag.KEY}  <KEY>      The key to set \n" +
            f"  {Flag.VALUE}  <VALUE>    The value to set \n" 
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

def handle_response(response, type, key):
    if response:
        if response.success:
            if type == Command.SET:
                print(f"\nSUCCESS: The key:value pair ({key}:{response.payload}) has been SET \n")
                
            elif type == Command.GET:
                if response.payload:
                    print(f"\nSUCCESS: Key: {key} - Value: {response.payload} \n")
                else:
                    print(f"\nNot value found for key: {key} \n")
        else:
            print(f"\nERROR: {response.error} \n")
    else:
        print("\nERROR: Response not provided \n")

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

    orign_key   = None

    argv = sys.argv
    argn = len(sys.argv) 

    if argn < 2 :
        usage()
        return

    command = argv[1]

    i = 2
    while i < argn:
        if (argv[i] == Flag.PORT and (argn-i) >= 2):
            i += 1
            client_port = int(argv[i])
        elif (argv[i] == Flag.NODEHOST and (argn-i) >= 2):
            i += 1
            node_host = str(argv[i])
        elif (argv[i] == Flag.NODEPORT and (argn-i) >= 2):
            i += 1
            node_port = int(argv[i])
        elif (argv[i] == Flag.KEY and (argn-i) >= 2):
            i += 1
            orign_key = argv[i]
            key = hash_val(str(argv[i])) # SHA
        elif (argv[i] == Flag.VALUE and (argn-i) >= 2):
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
    
    command = str.upper(command)

    try:
        client_address = client_host +':'+ str(client_port)
        client_id = hash_val(client_address)
        client = Client(client_id, client_host, client_port)

        node = create_node(node_host, node_port)

        if command == Command.CREATE:
            client.start()
        elif command == Command.JOIN:
            if node:
                client.join(node)
            else:
                print(joinUsage())
        elif command == Command.GET:
            if node and key:
                response = client.get(key, node)
                handle_response(response, command, orign_key)
            else:
                print(getUsage())
        elif command == Command.SET:
            if node and key != None and value:
                data = {'key': key,'value': value}
                response = client.set(data, node)
                handle_response(response, command, orign_key)
            else:
                print(setUsage())
        else:
            print(f"COMMAND: {command} NOT FOUND \n")
            usage()
    
    except Exception as e :
        print("ERROR MAIN: ", e)
    

if __name__ == '__main__':
    main()
    