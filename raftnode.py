import rpyc
import sys
from rpyc.utils.server import ThreadPoolServer
import time
import threading
import random
import os
import threading
import logging

def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.custom_attribute = "Node " + sys.argv[2]
    return record

logging.basicConfig(filename="logfile_" + sys.argv[2] + '.log', format="%(asctime)s - %(custom_attribute)s - %(message)s", filemode='w')
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
        if os.path.exists(self.term_file):
            logging.debug('getting term from file')
            with open(self.term_file, "r") as tf:
                self.term = int(tf.readline())
        else:
            logging.debug('Creating term file')
            self.term = 0
            with open(self.term_file, "w") as tf:
                tf.write(str(self.term))

        logging.debug("Current Term : [" + str(self.term) + "]")

        # initialize locks
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
        logging.debug("Initialization complete")
                        
    # increment term by 1 and store in the term file
    def update_term(self, new_term=None):
        old = self.term
        if(new_term == None):
            self.term += 1
        else:
            self.term = new_term

        logging.debug('Updating term - old : ' + str(old) + '| new : ' + str(self.term))
            
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
        logging.debug("Running server")
        # run indefinitely
        while True:
            self.state_lock.acquire()
            
            if(self.isLeader()):
                # if leader, send a heartbeat
                self.heartbeat()
            else:
                # check for timeout in follower or candidate
                if(self.is_timeout()):
                    # the timer timedout
                    logging.debug("Timout Detected")
                    self.become_candidate()
                elif self.state == self.CANDIDATE_STATE:
                    # if candidate in active election multicast to others
                    self.multicast_request()
                    
            # attempt to release the lock
            try:
                self.state_lock.release()
            except:
                pass

            time.sleep(.5)
            
    # multicast rpc
    def multicast_request(self):
        logging.debug('Starting request_vote multicast')
        # store current state information
        new_term = self.term
        old_term = new_term
        new_state = self.state
        active_election = True
        vote_count = self.vote_count
        self.state_lock.release()

        # assume nothing is wrong
        invalid = False

        # for every node
        for node in self.other_nodes_meta.values():
            # get their ip
            addr = node['hostname']
            port = int(node['port'])
            node_id = node['id']
            
            try:
                # send an requestvote rpc
                con = rpyc.connect(addr, port)
                valid, cur_term = con.root.request_vote(new_term, self.node_id)
                con.close()

                # if our term is less than the highest, we cannot be the leader anymore
                # we are now followers
                if not valid:
                    if(new_term > cur_term):
                        new_term = cur_term
                        new_state = self.FOLLOWER_STATE
                        invalid = True
                        break
                else:
                    # node votes for us
                    vote_count += 1
                    if(vote_count >= self.threshold):
                        # I am leader.
                        new_state = self.LEADER_STATE
                        active_election = False
                        break
            except:
                print("Could not connect to [" + addr + ':' + str(port) + "]")
                
        with self.state_lock:
            # if another node did not call an rpc on us with multicasting a heartbeat
            if self.term == old_term:
                logging.debug('No RPC called on us during requestVote multicast')
                self.print_self()                
                # if another node has higher term
                if(invalid):
                    # revert to follower
                    logging.debug('\trequestVote RPC failed. Reverting to follower')
                    self.update_term(new_term)
                    self.state = new_state
                    self.reset_timer()
                    self.reset_votes()
                    self.print_self()
                    self.active_election = False
                    self.print_self()
                # if i am leader
                elif(not active_election):
                    # declare self as leader
                    logging.debug('\trequestVote RPC success. I won election. I am leader')
                    logging.debug('Vote count [' + str(vote_count) +
                                  '] Threshold [' + str(self.threshold) + ']')
                    self.update_term(new_term)
                    self.state = new_state
                    self.reset_timer()
                    self.reset_votes()
                    self.update_term()
                    self.active_election = False
                    self.print_self()
                else:
                    logging.debug('\trequestVote RPC success. Continuing election')
                    if(self.vote_count == vote_count):
                        logging.debug('\tNode already voted')
                    self.vote_count = vote_count
                    self.print_self()
            else:
                logging.debug('RPC called on us during requestVote multicast. Cancelling result')
                self.print_self()

                
    def print_self(self):
        logging.debug("\t\tNode : " + str(self.node_id) + "- Term : "
                      + str(self.term) + " - State : " + str(self.state) + " - VCount : " + str(self.vote_count))
        
    # reset the votes
    def reset_votes(self):
        self.voted_for = None
        self.vote_count = 0
        
    # transitions our state to a candidate
    def become_candidate(self):
        logging.debug('Becoming a candidate')
        
        # update term & state
        self.update_term()
        self.state = self.CANDIDATE_STATE
        
        # display the current term
        logging.debug("Current Term : " + str(self.term))

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
        logging.debug('RequestVote RPC ' + str(can_term) + " " + str(can_id))
        # acquire the state lock
        with self.state_lock:
            # if our term is greater than the candidate
            if can_term < self.term:
                logging.debug('\tReject: Candidate ' + str(can_id))

                # reject the candidate
                return(False, self.term)
            # if can term is larger than our own
            elif(can_term > self.term):
                # convert to follower of candidate
                logging.debug('\tAccept: Candidate term larger - Voting for ' + str(can_id))

                self.update_term(can_term)
                self.state = self.FOLLOWER_STATE
                self.voted_for = can_id
                return(True, self.term)
            # if we have equal terms
            else:
                # candidate is part of the current election
                if self.voted_for is None:
                    # we have not yet voted for anyone or voted for the candidate already
                    logging.debug('\tAccept: Participant Voting for ' + str(can_id))
                    self.voted_for = can_id
                    
                    # vote for candidate
                    return(True, self.term)
                else:
                    logging.debug('\tReject: I have already voted ' + str(can_id))
                    # we have already voted
                    return(False, self.term)
                
    # multicast append entries to all nodes
    def heartbeat(self):
        logging.debug('Starting Heartbeat')
        # store current state information
        new_term = self.term
        old_term = new_term
        new_state = self.state
        self.state_lock.release()

        # assume nothing is wrong
        invalid = False
            
        # for every node
        for node in self.other_nodes_meta.values():
            # get their ip
            addr = node['hostname']
            port = int(node['port'])
            node_id = node['id']
            
            try:
                # send an appendentries rpc
                con = rpyc.connect(addr, port)
                valid, cur_term = con.root.append_entries(new_term, self.node_id)
                con.close()

                # if our term is less than the highest, we cannot be the leader anymore
                # we are now followers
                if not valid:
                    new_term = cur_term
                    new_state = self.FOLLOWER_STATE
                    invalid = True
                    break
            except:
                print("Could not connect to [" + addr + ':' + str(port) + "]")

        # securely update this nodes state
        with self.state_lock:
            # if another node did not call an rpc on us with multicasting a heartbeat
            if self.term == old_term:
                logging.debug('No RPC called on us during heartbeat')
                # if we should be followers
                if invalid:
                    logging.debug('Heartbeat Fail. Reverting to follower')
                    # update our state
                    self.update_term(new_term)
                    self.state = new_state
                    self.reset_timer()
                else:
                    logging.debug('Heartbeat success')
            else:
                logging.debug('RPC called on us during heartbeat. Cancelling result')            
                    
    # append entries rpc
    def exposed_append_entries(self, term, leaderId):
        logging.debug('AppendEntries RPC ' + str(term) + " " + str(leaderId))
        # lock our internal state
        with self.state_lock:
            # reject invalid requests
            if term < self.term:
                logging.debug('\tReject Append: Invalid leader detected')

                # notify of invalid term 
                return(False, self.term)
            else:
                logging.debug('\tAccept Append: Valid leader detected')
                
                # convert us to follower state
                self.state = self.FOLLOWER_STATE

                # reset our timer
                self.reset_timer()

                # debug if new leader or not
                if self.active_leader != leaderId:
                    logging.debug("\tAppend: New leader detected : [" + str(leaderId) + ']' +
                                  ' - old : [' + str(self.active_leader) + "]")
                else:
                    logging.debug('\tAppend: Same leader [' + str(leaderId) + ']')

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
    
    logging.debug('Server running')
    server = ThreadPoolServer(node, port=int(sys.argv[3]))
    server.start()
