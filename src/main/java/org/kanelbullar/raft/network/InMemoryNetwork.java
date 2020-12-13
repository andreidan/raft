package org.kanelbullar.raft.network;

import org.kanelbullar.raft.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InMemoryNetwork implements Network {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryNetwork.class);

    @Override
    public Map<String, RaftNode> createNodes(int numNodes) {
        Map<String, RaftNode> nodes = new ConcurrentHashMap<>(numNodes, 1.0f);
        List<String> nodeNames = IntStream.range(0, numNodes).boxed().map(index -> "node-" + index).collect(Collectors.toList());

        nodeNames.stream().forEach(selfName -> {
            nodes.put(selfName, new RaftNode(selfName, nodeNames.stream().filter(n -> !n.equals(selfName)).collect(Collectors.toList())));
        });
        return nodes;

    }

    @Override
    public void routeOutgoingMessages(Map<String, RaftNode> nodes) {
        for (Map.Entry<String, RaftNode> entry : nodes.entrySet()) {
            // route all outgoing messages form every node (source) to the corresponding destinations
            RaftNode source = entry.getValue();
            for (Message msg : source.outgoing) {
                RaftNode dest = nodes.get(msg.dest());
                if (dest == null) {
                    logger.error("unknown destination for message [{}]. dropping it", msg);
                } else {
                    dest.incoming.offer(msg);
                }
            }
            source.outgoing.clear();
        }
    }
}
