import rpyc
import sys
from rpyc.utils.server import ThreadPoolServer
import time
import threading
import random
import os
import threading
import logging


#logging.basicConfig(filename="newfile.log", 
#                    format='%(asctime)s %(message)s', 
#                    filemode='w')
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.custom_attribute = "Node " + sys.argv[2]
    return record

logging.basicConfig(filename="logfile.log", format="%(asctime)s - %(custom_attribute)s - %(message)s", filemode='w')
old_factory = logging.getLogRecordFactory()
logging.setLogRecordFactory(record_factory)
logger=logging.getLogger()

#Setting the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG) 

def run_asynch(temp):
    temp.run_server()

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
    threshold = None
    node_id = None
    port = None
    term = None
    hostname = None
    state_lock = None
    state = None
    timeout = None
    clock = None
    FOLLOWER_STATE = 0
    CANDIDATE_STATE = 1
    LEADER_STATE = 2
    active_leader = None
    term_file = None
    voted_for = None
    vote_nodes = []
    active_election = False
    
    '''
    Initialize the class using the config file provided and also initialize
    any datastructures you may need.
    '''        
    def __init__(self, config, node_id, port):
        # initialize node meta
        self.node_id = node_id
        self.port = port

        # find term file
        self.term_file = "./tmp/" + str(node_id) + "_term.txt"

        #get most up to date term or defaul to zero
        if os.path.exists(term_file):
            print('getting term from file')
            with open(self.term_file, "r") as tf:
                self.term = int(tf.readline())
        else:
            print('Creating term file')
            self.term = 0
            with open(term_file, "w") as tf:
                tf.write(str(self.term))

        print("Current Term : [" + str(self.term) + "]")

        # initialize locks
        self.leader_lock = threading.Lock()
        self.term_lock = threading.Lock()
        self.state_lock = threading.Lock()

        # initialize timers
        self.timeout = random.uniform(2,5)
        self.cur_clock = time.time()

        # initialize state
        self.state = self.FOLLOWER_STATE 

        # parse config
        with open(config, "r") as cfile:
            lines = cfile.readlines()
            
            for i, line in enumerate(lines, -1):
                cur_id, cur_addr = line.split()

                if i == -1:
                    # store total number of nodes
                    self.total_nodes = int(cur_addr)
                    self.threshold = int(self.total_nodes/2) + 1
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

    # increment term by 1 and store in the term file
    def update_term(self, new_term=None):
        if(new_term == None):
            self.term += 1
        else:
            self.term = new_term
            
        with open(self.term_file, "w") as tf:
            tf.write(str(self.term))

        
    # detects if our internal clock has timed out
    def is_timeout(self):
        now = time.time()

        time_passed = now - self.cur_clock

        if(time_passed > self.timeout):
            self.reset_timer()
            
            return True
        else:
            return False

    # resets the internal clock
    def reset_timer(self):
        self.cur_clock = time.time()

    def isLeader(self):
        return(self.state == self.LEADER_STATE)
    
    # continuously run
    def run_server(self):
        # run indefinitely
        while True:
            with self.state_lock:
                if self.active_election:
                    if self.voted_for == self.node_id:
                        # multicast to others
                        self.multicast_request()
            else:
                if(self.isLeader()):
                    # if leader, send a heartbeat
                    self.heartbeat()
                else:
                    # check for timeout in follower or candidate
                    if(self.is_timeout()):
                        # the timer timedout
                        print("Timout Detected")
                        self.become_candidate()
            

    # multicast rpc
    def multicast_request(self):
        # for every node
        for node in self.other_nodes_meta.values():
            # get their ip
            addr = node['hostname']
            port = int(node['port'])
            node_id = node['id']
            
            try:
                # send an requestvote rpc
                con = rpyc.connect(addr, port)
                valid, new_term = con.root.request_vote(self.term, self.node_id)
                con.close()

                # if our term is less than the highest, we cannot be the leader anymore
                # we are now followers
                if not valid and new_term > self.term:
                    self.update_term(new_term)
                    self.state = self.FOLLOWER_STATE
                    self.reset_timer()
                    break
                elif not node_id in vote_nodes:
                    vote_nodes.append(node_id)

                if(len(vote_nodes) >= self.threshold):
                    # Election complete. I am leader. Reset myself
                    print('Election completed. ' + str(self.node_id) + ' is leader')
                    self.state = self.LEADER_STATE
                    self.vote_nodes = []
                    self.reset_timer()
                    self.active_election = False
            except:
                print("Could not connect to [" + addr + ':' + str(port) + "]")

        
    # transitions our state to a candidate
    def become_candidate(self):
        print('Becoming a candidate')
        
        # update term & state
        self.update_term()
        self.state = self.CANDIDATE_STATE
        
        # display the current term
        print("Current Term : " + str(self.term))

        # perform leader election
        self.init_leader_election()

    # perform the leader election algorithm
    def init_leader_election(self):
        # vote for self
        self.vote_count = 1
        self.voted_for = self.node_id
        
        # reset election timer
        self.reset_timer()
        self.active_election = True
                
    # returns true if our state is leader
    def exposed_is_leader(self):
        return(self.isLeader())

    def exposed_request_vote(self, can_term, can_id):
        # acquire the state lock
        with self.state_lock:
            # if our term is greater than the candidate
            if can_term < self.term:
                # reject the candidate
                return(False, self.term)
            elif(can_term > self.term):
                # convert to follower of candidate
                self.active_election = True
                self.update_term(can_term)
                self.state = self.FOLLOWER_STATE
                self.voted_for = can_id
                return(True, self.term)
            else:
                # candidate is part of the election
                self.active_election = True
                if self.voted_for is None or self.voted_for is can_id:
                    # we have not yet voted for anyone or voted for the candidate already
                    self.voted_for = can_id
                    
                    # vote for candidate
                    return(True, self.term)
                else:
                    # we have already voted
                    return(False, self.term)
                
    # multicast append entries to all nodes
    def heartbeat(self):
        # for every node
        for node in self.other_nodes_meta.values():
            # get their ip
            addr = node['hostname']
            port = int(node['port'])
            node_id = node['id']
            
            try:
                # send an appendentries rpc
                con = rpyc.connect(addr, port)
                valid, new_term = con.root.append_entries(self.term, self.node_id)
                con.close()

                # if our term is less than the highest, we cannot be the leader anymore
                # we are now followers
                if not valid:
                    self.update_term(new_term)
                    self.state = self.FOLLOWER_STATE
                    self.reset_timer()
                    break
            except:
                print("Could not connect to [" + addr + ':' + str(port) + "]")

    # append entries rpc
    def exposed_append_entries(self, term, leaderId):
        # lock our internal state
        with self.state_lock:
            # reject invalid requests
            if term < self.term:
                print('Invalid leader detected')

                # notify of invalid term 
                return(False, self.term)
            else:
                print('Valid leader detected')
                
                # convert us to follower state
                self.state = self.FOLLOWER

                # reset our timer
                self.reset_timer()

                # debug if new leader or not
                if self.active_leader != leaderId:
                    print("New leader detected : [" + str(leaderId) + ']' +
                          ' - old : [' + str(self.active_leader) + "]")
                else:
                    print('Same leader [' + str(leaderId) + ']')

                # update the current leader
                self.active_leader = leaderId

                # notify of valid leader
                return(True, self.term)

if __name__ == '__main__':
    if(len(sys.argv) != 4):
        print("ERROR: Invalid number of params - Required : 4 - Given : " +
              str(len(sys.argv)))
        print("\tFormat: python3 raftnode.py config_file, node_id, port")
        
        exit(-1)

    node = RaftNode(config=sys.argv[1], node_id=int(sys.argv[2]), port=int(sys.argv[3]))
    print('Attempting to run server')

    t = threading.Thread(target=run_asynch, args=(node,))
    t.start()
    
    print('Server running')
    server = ThreadPoolServer(node, port=int(sys.argv[3]))
    print('Hello')
    server.start()
