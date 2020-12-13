package org.kanelbullar.raft.network;

public interface MessageRouter {

    void routeFrom(RaftNode source);

}
