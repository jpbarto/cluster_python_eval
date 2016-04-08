import zmq
import time
import threading
import json
import socket
from cluster import ClusterManager, Node

def stop_all ():
    clusterManager.stop ()
    ctx.term ()

def print_status ():
    print "Local node: {}, running: {}, connected: {}".format (clusterManager.node.name, clusterManager.running (), clusterManager.connected ())
    print "Connected Nodes: {}".format (clusterManager.nodes)

ctx = zmq.Context (1)
clusterManager = ClusterManager (ctx, 5560, 5570)
clusterManager.start ()
