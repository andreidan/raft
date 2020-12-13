package org.kanelbullar.raft.network;

import org.junit.Test;
import org.kanelbullar.raft.message.Message;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InMemoryNetworkTest {

    @Test
    public void testNetwork() {
        InMemoryNetwork inMemoryNetwork = new InMemoryNetwork();
        Map<String, RaftNode> nodes = inMemoryNetwork.createNodes(2);
        Iterator<RaftNode> nodeIterator = nodes.values().iterator();
        RaftNode nodeZero = nodeIterator.next();
        RaftNode nodeOne = nodeIterator.next();

        nodeZero.send(new Message(nodeZero.name, nodeOne.name, "test payload"));
        inMemoryNetwork.routeOutgoingMessages(nodes);

        Message message = nodeOne.deliver();
        assertEquals(nodeZero.name, message.source());
        assertEquals(nodeOne.name, message.dest());
        assertEquals("test payload", message.payload());
    }

}