package org.kanelbullar.raft.network;

import java.util.Map;

public interface Network {

    Map<String, RaftNode> createNodes(int numNodes);

    void routeOutgoingMessages(Map<String, RaftNode> nodes);

    // TODO route incoming meesages explicitly too - this happens when creating the nodes now (ie. listening)
}
