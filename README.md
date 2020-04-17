# raft_distributed_consensus
CIS 4307 - Final Project - Luke Jakielaszek - 915377673 - tug52339

## Application Overview
The application inherits from the rpyc service. Therefore, the requestvote and appendentries are implemented as exposed methods. All rpc calls come through the rpyc threadpoolserver instance. The leader election and heartbeat logic is implemented through a secondary thread which calls run_asynch/run_server methods. To ensure all information remains consistent across threads, I implemented a mutex which locks the object anytime we update state/term info. However, to prevent deadlock, during heartbeats and multicasting requestvotes, I stored the servers state info in a temporary variable, performed the multicasting, then re-acquired the lock for the server and checked if our state had changed while multicasting (another server calls an rpc on us). If our state had changed, I throw out the results from the multicast/heartbeat, otherwise I securely update the state/term/election info for the object and continue as normal.

### RequestVoteRPC
The requestvote rpc models takes in the candidates term and id. Before doing anything, it acuqires the nodes lock, ensuring that updates are synchronized. It rejects the candidate if the current term is higher than the candidates or the node has already voted. It accepts the candidate if the candidates term is larger than its current term or it has not yet voted during the current election.

### AppendEntries RPC
The appendentries rpc models takes in the leaders term and id. Before doing anything, it acuqires the nodes lock, ensuring that updates are synchronized. It rejects the appendentries if the current term is higher than the leaders. It accepts the appendentries in all other cases and resets its timer.

### Heartbeat
If a server is the leader, it will send out a heartbeat. At the start of the heartbeat, the server object is locked and the state / term info is stored in temporary variables. Then the server is unlocked. This avoids deadlock in the distributed system when calling appendentries on other nodes in the network. The appendentries is then multicasted to all other nodes in the network. If false is returned, the leaders temporary state is set to follower and the term is updated. After multicasting, the leader will acquire its own lock and check if any rpcs were called on it during multicasting that affected its state. If not, it securely updates its state information that was gathered during the heartbeat. If so, it throws out the results from the heartbeat.

### Multicast
If a server is a candidate, it will send out a multicast requestVote rpc. At the start of this multicast, the server object is locked and the state / term info is stored in temporary variables. Then the server is unlocked. This avoids deadlock in the distributed system when calling requestVote rpcs on other nodes in the network. The requestVote is then multicasted to all other nodes in the network. If false is returned, the current node checks if the nodes term is higher than its temporary term. If so, it converts its temporary states to follower. otherwise, it ignores the result as the node has already voted for someone. If True is returned, it increments its temporary count of voted nodes and checks to see if it has acquired more than half of the votes for the current election (and therefore become leader). After multicasting, the candidate will acquire its own lock and check if any rpcs were called on it during multicasting that affected its state. If not, it securely updates its state information that was gathered during the heartbeat. If so, it throws out the results from the multicast requestVote rpc.

## Limitations
As we have only implemented the leader election portion of the protocol, there is no log replication or meaningful message transfer. In addition, I chose to only save the current term to file rather than the entire state of a server. I chose to do this to limit the occurrence of non-byzantine failures and avoid conflict when updating a file. If a node fails, it will therefore start again as a follower from its current term, then timeout and start an election or be picked up by another leader. By not storing state, the raft protocol will just start a new election on timeout and proceed as normal. However, if I were to store state in addition to term, the probability of having a failure while updating a state file would increase and possibly lead to multiple nodes being leader during a single term, which should be guarenteed to not happen in raft.

For example, if our file stores data in format term,state such as below:
1,leader

If we went to update both the term and state after receiving an RequestVote RPC from another node to start a new election, we would expect the following:
2,candidate

However, if we killed a node partway through this update, we may only have been able to write the term, but not update the state and get the following:
2,leader

Therefore, on restart the node would declare itself the leader of a term in which it never won an election, which is bad. To avoid this, I only stored term and defaulted to follower on restart.