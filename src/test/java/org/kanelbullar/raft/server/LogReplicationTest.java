package org.kanelbullar.raft.server;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kanelbullar.raft.log.LogEntry;
import org.kanelbullar.raft.log.RaftLog;
import org.kanelbullar.raft.network.AddressPort;
import org.kanelbullar.raft.network.RaftNode;
import org.kanelbullar.raft.network.TCPRaftNetwork;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogReplicationTest {

    private ExecutorService networkExecutor;
    private int numNodes;
    private ExecutorService messageProcessingThreads;
    private TCPRaftNetwork network;

    @Before
    public void setupExecutors() {
        networkExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            int count = 0;

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "network-" + (count++));
            }
        });
        numNodes = 5;
        messageProcessingThreads = Executors.newCachedThreadPool(new ThreadFactory() {
            int count = 0;

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "message_proc-" + (count++));
            }
        });

        network = new TCPRaftNetwork(networkExecutor, Map.of(
                "node-0", new AddressPort("localhost", 7000),
                "node-1", new AddressPort("localhost", 7001),
                "node-2", new AddressPort("localhost", 7002),
                "node-3", new AddressPort("localhost", 7003),
                "node-4", new AddressPort("localhost", 7004)
        ));
    }

    @After
    public void teardownExecutors() {
        try {
            networkExecutor.awaitTermination(2, TimeUnit.SECONDS);
            messageProcessingThreads.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testLogReplication() throws InterruptedException {
        Map<String, RaftNode> nodes = network.createNodes(numNodes);

        Map<String, RaftServer> servers = new HashMap<>();
        for (Map.Entry<String, RaftNode> entry : nodes.entrySet()) {
            RaftServer raftServer = new RaftServer(entry.getValue());
            servers.put(entry.getKey(), raftServer);
            messageProcessingThreads.submit(raftServer::deliverMessages);
        }

        network.routeOutgoingMessages(nodes);

        RaftServer leader = servers.get("node-0");
        leader.becomeLeader();
        leader.clientAppendEntry("testing");

        // TODO oh boy, cringe++, need a better way to wait for delivery
        int retryCount = 10;
        boolean entryReplicated = false;
        while (--retryCount > 0 && entryReplicated == false) {
            boolean allNodesReceivedEntry = true;
            for (RaftServer server : servers.values()) {
                if (server.log.length() == 0) {
                    allNodesReceivedEntry = false;
                    break;
                }
            }
            entryReplicated = allNodesReceivedEntry;
            Thread.sleep(100);
        }

        RaftLog leaderLog = leader.log;
        for (RaftServer server : servers.values()) {
            assertEquals(server.self.name + " doesn't match the leader log", leaderLog, server.log);
            assertEquals(1, server.log.get(0).term);
            assertEquals("testing", server.log.get(0).command);
        }
    }

    @Test
    public void testLogReplicationFillsInGaps() {
        numNodes = 2;
        TCPRaftNetwork network = new TCPRaftNetwork(networkExecutor, Map.of(
                "node-0", new AddressPort("localhost", 7000),
                "node-1", new AddressPort("localhost", 7001)
        ));

        Map<String, RaftNode> nodes = network.createNodes(numNodes);

        Map<String, RaftServer> servers = new HashMap<>();
        for (Map.Entry<String, RaftNode> entry : nodes.entrySet()) {
            RaftServer raftServer = new RaftServer(entry.getValue());
            servers.put(entry.getValue().name, raftServer);
            messageProcessingThreads.submit(raftServer::deliverMessages);
        }

        network.routeOutgoingMessages(nodes);

        RaftServer leader = servers.get("node-0");
        // leader and first follower from Figure 6 https://raft.github.io/raft.pdf
        assertTrue(leader.log.appendEntries(-1, -1, List.of(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y")
        )));
        assertTrue(leader.log.appendEntries(2, 1, List.of(
                new LogEntry(2, "n")
        )));
        assertTrue(leader.log.appendEntries(3, 2, List.of(
                new LogEntry(3, "y"),
                new LogEntry(3, "y"),
                new LogEntry(3, "y"),
                new LogEntry(3, "y")
        )));

        RaftServer follower = servers.get("node-1");
        assertTrue(follower.log.appendEntries(-1, -1, List.of(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y")
        )));
        assertTrue(follower.log.appendEntries(2, 1, List.of(
                new LogEntry(2, "n")
        )));
        assertTrue(follower.log.appendEntries(3, 2, List.of(
                new LogEntry(3, "y")
        )));

        // now we'll append another entry to the leader who should fill in the gaps the
        // follower has in the log (6->8 and also replicate this new entry)
        leader.setCurrentTerm(3);
        leader.becomeLeader();
        leader.clientAppendEntry("new entry");

        int retriesCount = 10;
        while (follower.log.length() < 9 && --retriesCount > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        assertEquals("the leader log must be replicated to all servers", leader.log, follower.log);
    }

    @Test
    public void testReplicationBacktrack_ScenarioSlide137() {
        Map<String, RaftNode> nodes = network.createNodes(numNodes);

        Map<String, RaftServer> servers = new HashMap<>();
        for (Map.Entry<String, RaftNode> entry : nodes.entrySet()) {
            RaftServer raftServer = new RaftServer(entry.getValue());
            servers.put(entry.getValue().name, raftServer);
            messageProcessingThreads.submit(raftServer::deliverMessages);
        }

        network.routeOutgoingMessages(nodes);

        servers.get("node-0").log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0"),
                new LogEntry(3, "y<7"),
                new LogEntry(3, "x<5"),
                new LogEntry(3, "x<4")

        );

        servers.get("node-1").log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0")
        );

        servers.get("node-2").log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0"),
                new LogEntry(3, "y<7"),
                new LogEntry(3, "x<5"),
                new LogEntry(3, "x<4")
        );

        servers.get("node-3").log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1")
        );

        servers.get("node-4").log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0"),
                new LogEntry(3, "y<7"),
                new LogEntry(3, "x<5")
        );

        RaftServer leader = servers.get("node-2");
        leader.setCurrentTerm(4);
        leader.becomeLeader();
        leader.clientAppendEntry("y<7");

        // TODO oh boy, cringe++, need a better way to wait for delivery
        int retriesCount = 20;
        boolean allLogLengthsAreNine = false;
        while (--retriesCount > 0 && allLogLengthsAreNine == false) {
            boolean foundLengthDifferentToNine = false;
            for (RaftServer server : servers.values()) {
                if (server.log.length() != 9) {
                    foundLengthDifferentToNine = true;
                    break;
                }
            }
            allLogLengthsAreNine = !foundLengthDifferentToNine;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // TODO: add testing infrastructure to assert the `prevIndex` that succeeded

        for (RaftServer server : servers.values()) {
            assertEquals("the leader log must be replicated to all servers, but failed on: " + server.self.name,
                    leader.log, server.log);
        }

    }

}
