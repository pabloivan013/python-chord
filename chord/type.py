import enum

class Type(str, enum.Enum):
    NOTIFY           = 'NOTIFY'
    GET_CPF          = 'GET_CPF'
    GET_KEYS         = 'GET_KEYS'
    SET_DATA         = 'SET_DATA'
    GET_DATA         = 'GET_DATA'
    REPLICATION      = 'REPLICATION'
    CHECK_STATUS     = 'CHECK_STATUS'
    GET_SUCCESSOR    = 'GET_SUCCESSOR'
    FIND_SUCCESSOR   = 'FIND_SUCCESSOR'
    GET_PREDECESSOR  = 'GET_PREDECESSOR'
    FIND_PREDECESSOR = 'FIND_PREDECESSOR'