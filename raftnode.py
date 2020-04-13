import rpyc
import sys
from rpyc.utils.server import ThreadPoolServer
import time
import threading
import random
import os

'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''

class RaftNode(rpyc.Service):
    other_nodes_meta = {}
    total_nodes = None
    node_id = None
    port = None
    term = None
    hostname = None
    is_leader = False
    leader_lock = None
    term_lock = None
    timeout = None
    clock = None
    FOLLOWER_STATE = 0
    CANDIDATE_STATE = 1
    LEADER_STATE = 2
    
    '''
    Initialize the class using the config file provided and also initialize
    any datastructures you may need.
    '''        
    def __init__(self, config, node_id, port):
        self.node_id = node_id
        self.port = port
        term_file = "./tmp/" + str(node_id) + "_term.txt"
        if os.path.exists(term_file):
            print('getting term from file')
            with open(term_file, "r") as tf:
                self.term = int(tf.readline())
        else:
            print('Creating term file')
            self.term = 0
            with open(term_file, "w") as tf:
                tf.write(str(self.term))

        print("Current Term : [" + str(self.term) + "]")
        self.leader_lock = threading.Lock()
        self.term_lock = threading.Lock()
        self.timeout = random.randint(2,5)
        self.cur_clock = time.time()
        self.state = self.FOLLOWER_STATE 
        
        with open(config, "r") as cfile:
            lines = cfile.readlines()
            
            for i, line in enumerate(lines, -1):
                cur_id, cur_addr = line.split()

                if i == -1:
                    # store total number of nodes
                    self.total_nodes = int(cur_addr)
                else:
                    hostname, cur_port = cur_addr.split(":")
                    cur_port = int(cur_port)
                    if i == self.node_id:
                        # store our node meta
                        assert(self.port == cur_port)
                        self.hostname = hostname
                    else:
                        # store other node info
                        self.other_nodes_meta[i] = {}
                        self.other_nodes_meta[i]["hostname"] = hostname
                        self.other_nodes_meta[i]["port"] = cur_port
                        self.other_nodes_meta[i]["id"] = i
                        self.other_nodes_meta[i]['con'] = None

        self.run_server()
        
    def display_others(self):
        for key in self.other_nodes_meta:
            cur_dict = self.other_nodes_meta[key]
            print("Current ID : [" + str(key) + "]")
            print("\tHostname : [" + str(cur_dict["hostname"]) + "]")
            print("\tPort : [" + str(cur_dict["port"]) + "]")
            print("\tID : [" + str(cur_dict["id"]) + "]")
            print("\tCon : [" + str(cur_dict["con"]) + "]")
            
    def display_self(self):
        print("This Node : [" + str(self.node_id) + "]")
        print("\tPort : [" + str(self.port) + "]")
        print("\tHostname : [" + str(self.hostname) + "]")
        print("\tID : [" + str(self.node_id) + "]")        
        print("Total Nodes : [" + str(self.total_nodes) + "]")
        
        self.display_others()

    # continuously run 
    def run_server(self):
        print('Running Server')
        for i in range(self.total_nodes):

            
        
    '''
    x = is_leader(): returns True or False, depending on whether
    this node is a leader
    
    As per rpyc syntax, adding the prefix 'exposed_' will expose this
    method as an RPC call

    CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''
    def exposed_is_leader(self):
        return(self.is_leader)

    def exposed_request_vote(self):
        print("Request Vote RPC")
        pass

    def exposed_append_entries(self):
        print("Append Entries RPC")
        pass

    def on_connect(self, con):
        print("Connected")

    def on_disconnect(self, con):
        print("disconnected")

if __name__ == '__main__':
    if(len(sys.argv) != 4):
        print("ERROR: Invalid number of params - Required : 4 - Given : " +
              str(len(sys.argv)))
        print("\tFormat: python3 raftnode.py config_file, node_id, port")
        
        exit(-1)
        
    server = ThreadPoolServer(RaftNode(config=sys.argv[1], node_id=int(sys.argv[2]),
                                       port=int(sys.argv[3])), port=int(sys.argv[3]))
    server.start()
