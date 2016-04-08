import zmq
import time
import threading
import json

name = 'bob'

def cluster_manager (context, join_uri):
    nodes = [name]
    join_sock = context.socket (zmq.REP)
    join_sock.bind (join_uri)

    while True:
        message = join_sock.recv ()
        req = json.loads (message)
        if 'type' in req and req['type'] == 'JOIN' and req['node'] not in nodes:
            nodes.append (req['node'])

        resp = {'type': 'ACK', 'nodes': nodes}
        join_sock.send (json.dumps(resp))

ctx = zmq.Context (1)

thread = threading.Thread (target = cluster_manager, args = (ctx, 'tcp://*:5560'))
thread.start ()
