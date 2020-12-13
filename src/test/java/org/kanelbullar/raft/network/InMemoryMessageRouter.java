package org.kanelbullar.raft.network;

import org.jetbrains.annotations.TestOnly;
import org.kanelbullar.raft.message.PoisonPill;
import org.kanelbullar.raft.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class InMemoryMessageRouter {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryMessageRouter.class);

    /**
     * Simulates message routing by routing the outgoing messages for all the servers
     * to the corresponding servers incoming queues, and then it delivers/handles the
     * messages in the incoming queues.
     * Rinse and repeat until there's no more traffic.
     */
    @TestOnly
    public static void runMessages(Network network, Map<String, RaftNode> nodes, RaftServer[] servers) {
        while (anyNodeHasOutgoings(nodes.values())) {
            network.routeOutgoingMessages(nodes);
            // enqueue a poison so the delivery below doesn't block forever (waiting for more messages)
            for (RaftNode node : nodes.values()) {
                logger.debug("InMemoryMessageRouter enqueued poison pill to [{}] : [{}]", node.name,
                        node.incoming.offer(new PoisonPill(node.name, node.name)));

            }
            for (RaftServer server : servers) {
                logger.debug("InMemoryMessageRouter - delivering messages on [{}]", server);
                server.deliverMessages();
            }
        }
    }

    private static boolean anyNodeHasOutgoings(Collection<RaftNode> nodes) {
        for (RaftNode value : nodes) {
            if (value.outgoing.size() > 0) {
                return true;
            }
        }
        return false;
    }
}
