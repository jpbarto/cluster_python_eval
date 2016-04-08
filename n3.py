import zmq
import time
import threading
import json
import socket
from cluster import Cluster, Node
from pprint import pprint

def stop_all ():
    cluster.stop ()
    time.sleep (1)
    ctx.term ()

def print_status ():
    pprint (cluster.status_report (), indent=4)

ctx = zmq.Context (1)
node = Node (5760, 5770, 'node3')
cluster = Cluster (node, ctx)
cluster.start ()
