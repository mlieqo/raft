RAFT_NODES = {
    0: ('localhost', 10000),
    1: ('localhost', 11000),
    2: ('localhost', 12000),
    3: ('localhost', 13000),
    4: ('localhost', 14000),
}

KV_STORE = {
    0: ('localhost', 10001),
    1: ('localhost', 11001),
    2: ('localhost', 12001),
    3: ('localhost', 13001),
    4: ('localhost', 14001),
}


# Aiomanhole configuration
# By default the aiomanhole is accessible only via unix socket
AIOMANHOLE_HOST = 'localhost'
AIOMANHOLE_PORT = 9999
AIOMANHOLE_SOCKET = '/tmp/raft_aiomanhole.sock'
