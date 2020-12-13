package org.kanelbullar.raft.network;

import org.junit.Test;
import org.kanelbullar.raft.message.Message;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TCPRaftNetworkTest {

    @Test
    public void testBasicSendReceive() throws InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            TCPRaftNetwork network = new TCPRaftNetwork(executorService, Map.of(
                    "node-0", new AddressPort("localhost", 7000),
                    "node-1", new AddressPort("localhost", 7001),
                    "node-2", new AddressPort("localhost", 7002),
                    "node-3", new AddressPort("localhost", 7003),
                    "node-4", new AddressPort("localhost", 7004)
            ));
            Map<String, RaftNode> nodes = network.createNodes(5);

            network.routeOutgoingMessages(nodes);

            RaftNode nodeZero = nodes.get("node-0");
            RaftNode nodeOne = nodes.get("node-1");

            nodeZero.send(new Message(nodeZero.name, nodeOne.name, "test payload"));
            Message message = nodeOne.deliver();
            assertEquals(message.source(), nodeZero.name);
            assertEquals(message.dest(), nodeOne.name);
            assertEquals("test payload", message.payload());

            Message anotherDeliveredMessage = nodeOne.deliver(2, TimeUnit.SECONDS);
            assertNull(anotherDeliveredMessage);

            nodeZero.send(new Message(nodeZero.name, nodeOne.name, "1"));
            nodeZero.send(new Message(nodeZero.name, nodeOne.name, "2"));
            nodeZero.send(new Message(nodeZero.name, nodeOne.name, "3"));

            var msgOne = nodeOne.deliver();
            assertEquals("1", msgOne.payload());
            String replyPayload = "reply to message 1 [" + msgOne.payload() + "]";
            nodeOne.send(new Message(nodeOne.name, msgOne.source(), replyPayload));
            var replyToFirstMsg = nodeZero.deliver();
            assertEquals(replyPayload, replyToFirstMsg.payload());

            // deliver msg 2 and 3 on nodeZero
            assertEquals("2", nodeOne.deliver().payload());
            assertEquals("3", nodeOne.deliver().payload());
        } finally {
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

}
