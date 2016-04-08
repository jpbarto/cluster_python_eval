import zmq
import time
import json

ctx = zmq.Context (1)
sock = ctx.socket (zmq.REQ)
sock.connect ("tcp://127.0.0.1:5560")

def join_cluster (name):
    sock.send_json ({'type': 'JOIN', 'node': name, 'timestamp': time.time ()})
    # sock.send (json.dumps ({'type': 'JOIN', 'node': name, 'timestamp': time.time ()}))
    resp = sock.recv_json ()
    print ("Response {}".format (resp))
