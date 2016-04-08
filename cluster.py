import zmq
import time
import threading
import json
import socket

class Node:
    name = ''
    ipaddr = ''
    cmd_port = 0
    event_port = 0
    details = None

    def __init__ (self, cmd_port, event_port, name = ''):
        self.ipaddr = socket.gethostbyname_ex (socket.gethostname ())[2][0]
        self.cmd_port = cmd_port
        self.event_port = event_port
        if name == '':
            self.name = self.ipaddr
        else:
            self.name = name
        self.details = {'name': self.name, 'ipaddr': self.ipaddr, 'command': "{}:{}".format (self.ipaddr, cmd_port), 'event': '{}:{}'.format (self.ipaddr, event_port)}

class Cluster:
    nodes = None
    local_node = None

    ctx = None
    cmdListener = None
    evtListener = None
    pubSocket = None
    
    def __init__ (self, node, context):
        self.local_node = node
        self.nodes = [node.details]
        self.ctx = context

        self.cmdListener = ClusterCommandListener (context, self)
        self.evtListener = ClusterEventListener (context, self)

        self.pubSocket = self.ctx.socket (zmq.PUB)
        self.heartbeat = HeartbeatAgent (self.pubSocket, self)

    def start (self):
        self.pubSocket.bind ('tcp://{}:{}'.format (self.local_node.ipaddr, self.local_node.event_port))

        self.cmdListener.start ()
        self.evtListener.start ()
        self.heartbeat.start ()

    def stop (self):
        self.heartbeat.stop ()
        self.cmdListener.stop ()
        self.evtListener.stop ()
        self.pubSocket.close ()

    def connected (self):
        return self.cmdListener.connected ()

    def running (self):
        return self.cmdListener.running () and self.evtListener.running ()

    def add_node (self, node):
        if node not in self.nodes:
            self.nodes.append (node)
            self.evtListener.add_subscription (node)
            self.pubSocket.send_json ({'type': 'UPDATE', 'node': self.local_node.details, 'nodes': self.nodes, 'timestamp': time.time ()})

    def remove_node (self, node):
        if node in self.nodes:
            self.nodes.remove (node)
            self.evtListener.remove_subscription (node)
            self.pubSocket.send_json ({'type': 'UPDATE', 'node': self.local_node.details, 'nodes': self.nodes, 'timestamp': time.time ()})

    def join_cluster (self, cluster_cmd_uri):
        socket = self.ctx.socket (zmq.REQ)
        socket.connect ('tcp://{}'.format (cluster_cmd_uri))
        socket.send_json ({'type': 'JOIN', 'node': self.local_node.details, 'timestamp': time.time ()})
        resp = socket.recv_json ()

        for node in resp['nodes']:
            self.add_node (node)

        socket.close ()

    def status_report (self):
        ret = {'local_node': self.local_node.details,
                'cluster_nodes': self.nodes,
                'command_listener': {
                    'command_uri': self.local_node.details['command'],
                    'running': self.cmdListener.running (),
                    'connected': self.cmdListener.connected ()
                },
                'event_listener': {
                    'event_uri': self.local_node.details['event'],
                    'running': self.evtListener.running (),
                    'sockets': self.evtListener.sockets.keys ()
                }
        }
        return ret                    

class HeartbeatAgent:
    runFlag = False

    cluster = None
    pubSocket = None

    def __init__ (self, socket, cluster):
        self.pubSocket = socket
        self.cluster = cluster

    def start (self):
        self.runFlag = True
        self.thread = threading.Thread (target = self.run, args = ())
        self.thread.start ()

    def stop (self):
        self.runFlag = False

    def run (self):
        while self.runFlag:
            self.pubSocket.send_json ({'type': 'HEARTBEAT', 'node': self.cluster.local_node.details, 'timestamp': time.time ()})
            time.sleep (1)

class ClusterCommandListener:
    runFlag = False
    thread = None

    ctx = None
    sock = None

    cluster = None

    def __init__ (self, context, cluster):
        self.cluster = cluster

        self.ctx = context
        self.sock = self.ctx.socket (zmq.REP)

    def start (self):
        self.sock.bind ('tcp://{}:{}'.format (self.cluster.local_node.ipaddr, self.cluster.local_node.cmd_port))
        self.runFlag = True
        self.thread = threading.Thread (target = self.run, args = ())
        self.thread.start ()

    def run (self):
        while self.runFlag:
            try:
                req = self.sock.recv_json (zmq.NOBLOCK)
                if 'type' in req and req['type'] == 'JOIN':
                    self.cluster.add_node (req['node'])
                self.sock.send_json ({'type': 'ACK', 'nodes': self.cluster.nodes, 'timestamp': time.time ()})
            except zmq.ZMQError:
                time.sleep (1)
            except Exception as ex:
                print ex

        self.sock.close ()

    def stop (self):
        self.runFlag = False

    def connected (self):
        return not self.sock.closed

    def running (self):
        return self.runFlag

class ClusterEventListener:
    runFlag = False
    thread = None

    ctx = None
    poller = None

    cluster = None
    sockets = None

    hb_monitor = None

    def __init__ (self, context, cluster):
        self.cluster = cluster

        self.ctx = context
        self.poller = zmq.Poller ()

        self.sockets = {}
        self.hb_monitor = {}

    def start (self):
        self.runFlag = True
        self.thread = threading.Thread (target = self.run, args = ())
        self.thread.start ()

    def run (self):
        while self.runFlag:
            socks = dict(self.poller.poll (1000))
            for key in socks.keys ():
                if socks[key] == zmq.POLLIN:
                    message = key.recv_json ()
                    if 'type' in message and message['type'] == 'UPDATE':
                        for node in message['nodes']:
                            self.cluster.add_node (node)
                    elif 'type' in message and message['type'] == 'HEARTBEAT':
                        node = message['node']
                        node_name = node['name']
                        if node_name not in self.hb_monitor:
                            self.hb_monitor[node_name] = 0
                        self.hb_monitor[node_name] = time.time ()

            # check if we've not received a heartbeat from any nodes
            curr_time = time.time ()
            for node in self.cluster.nodes:
                node_name = node['name']
                if node_name == self.cluster.local_node.name:
                    continue

                if node_name not in self.hb_monitor:
                    self.hb_monitor[node_name] = curr_time 
                if (curr_time - self.hb_monitor[node_name]) > 3:
                    self.cluster.remove_node (node)

        for socket in self.sockets.values ():
            self.poller.unregister (socket)
            socket.close ()

    def add_subscription (self, node):
        sub_uri = 'tcp://{}'.format (node['event'])
        if sub_uri in self.sockets:
            return

        socket = self.ctx.socket (zmq.SUB)
        socket.connect (sub_uri)
        socket.setsockopt (zmq.SUBSCRIBE, '')
        self.poller.register (socket, zmq.POLLIN)
        self.sockets[sub_uri] = socket

    def remove_subscription (self, node):
        sub_uri = 'tcp://{}'.format (node['event'])
        if sub_uri not in self.sockets:
            return

        socket = self.sockets.pop (sub_uri)
        self.poller.unregister (socket)
        socket.close ()

    def stop (self):
        self.runFlag = False

    def running (self):
        return self.runFlag
