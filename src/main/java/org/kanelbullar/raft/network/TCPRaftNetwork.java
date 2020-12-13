package org.kanelbullar.raft.network;

import org.kanelbullar.raft.message.Message;
import org.kanelbullar.raft.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.kanelbullar.raft.transport.Transport.receiveMessage;

// TODO this needs a shutdown/decommission method to close all the sockets and gracefully shutdown
public class TCPRaftNetwork implements Network {

    private static final Logger logger = LoggerFactory.getLogger(TCPRaftNetwork.class);
    private static final int BASE_PORT = 7000;

    private final ExecutorService executorService;
    private final Map<String, AddressPort> network = new ConcurrentHashMap<>();

    public TCPRaftNetwork(ExecutorService executorService, Map<String, AddressPort> nodesAndPorts) {
        this.executorService = executorService;
        for (Map.Entry<String, AddressPort> entry : nodesAndPorts.entrySet()) {
            network.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Listens for incoming connections on the node's port, receives the message from the socket
     * and "receives" it on the node (queueing it up as incoming on the node).
     */
    private void listenAndReceiveRequests(RaftNode node, int port) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            logger.debug("listen on node [{}] port [{}]", node.name, port);
            while (true) {
                final Socket clientSocket = serverSocket.accept(); // blocking
                logger.debug("connection accepted [{}]", clientSocket);
                executorService.submit(() -> {
                    try {
                        while (true) {
                            var msg = receiveMessage(clientSocket);
                            node.receive(msg);
                        }
                    } finally {
                        clientSocket.close();
                    }
                });
            }
        }
    }

    /**
     * Maintains an open socket from the source to all the encountered destinations
     * and sends the source outgoing messages over the corresponding socket to the message destination
     */
    private void routeOutgoingMessages(final RaftNode source, Map<String, AddressPort> network) {
        final Map<String, Socket> clientOpenSockets = new ConcurrentHashMap<>();
        while (true) {
            Message msg;
            try {
                msg = source.outgoing.take(); // can block
            } catch (InterruptedException e) {
                // try again
                continue;
            }

            String destName = msg.dest();
            AddressPort destNode = network.get(msg.dest());
            String destAddress = destNode.address;
            int port = destNode.port;

            Socket socket = clientOpenSockets.computeIfAbsent(destName, (dest) -> {
                try {
                    logger.debug("[{}] opened connection to [{}] on port [{}]", source.name, destAddress, port);
                    return new Socket(destAddress, port);
                } catch (IOException e) {
                    logger.error("[{}] unable to open socket to address [{}] on port [{}] due to [{}]", source.name, destAddress, port,
                            e.getMessage());
                }
                return null;
            });
            if (socket != null) {
                try {
                    Transport.sendMessage(socket, msg);
                } catch (IOException e) {
                    logger.error("[{}] unable to send message [{}] due to [{}]", source.name, msg, e.getMessage());
                    if (socket.isClosed() || socket.isConnected() == false) {
                        // if the IO error yields the socket unusable, remove it from the open sockets map so it
                        // can be rebuilt/reconnected next time we want to send a message to this destination
                        clientOpenSockets.remove(destName);
                    }
                }
            } else {
                logger.warn("[{}] null socket detected. message [{}] will not be sent", source.name, msg);
            }
        }
    }

    @Override
    public Map<String, RaftNode> createNodes(int numNodes) {
        if (network.isEmpty() == false && network.size() != numNodes) {
            throw new IllegalArgumentException("the requested number of nodes [" + numNodes + "] to create doesn't match the number " +
                    "of " +
                    "nodes configured in the network:" + network.size());
        }
        Map<String, RaftNode> nodes = new ConcurrentHashMap<>(numNodes, 1.0f);
        List<String> nodeNames = IntStream.range(0, numNodes).boxed().map(index -> "node-" + index).collect(Collectors.toList());
        int portOffset = 0;
        if (network.isEmpty()) {
            for (String nodeName : nodeNames) {
                network.put(nodeName, new AddressPort("localhost", BASE_PORT + portOffset++));
            }
        } else {
            // validate the addresses preconfigured in the network match the addresses we would create (these will have to be
            // configurable)
            for (String nodeName : nodeNames) {
                if (network.get(nodeName) == null) {
                    throw new IllegalArgumentException("node [" + nodeName + "] is not configured in the network");
                }
            }
        }

        nodeNames.stream().forEach(selfName -> {
            RaftNode newNode = new RaftNode(selfName, nodeNames.stream().filter(n -> !n.equals(selfName)).collect(Collectors.toList()));
            nodes.put(selfName, newNode);

            executorService.submit(() -> {
                while (true) {
                    try {
                        listenAndReceiveRequests(newNode, network.get(selfName).port);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
        });
        return nodes;
    }

    @Override
    public void routeOutgoingMessages(Map<String, RaftNode> nodes) {
        for (Map.Entry<String, RaftNode> nodeEntry : nodes.entrySet()) {
            executorService.submit(() -> {
                routeOutgoingMessages(nodeEntry.getValue(), network);
            });
        }
    }
}
