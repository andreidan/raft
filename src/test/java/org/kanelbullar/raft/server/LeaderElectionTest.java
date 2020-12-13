package org.kanelbullar.raft.server;

import org.junit.Test;
import org.kanelbullar.raft.log.LogEntry;
import org.kanelbullar.raft.network.InMemoryNetwork;
import org.kanelbullar.raft.network.Network;
import org.kanelbullar.raft.network.RaftNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.kanelbullar.raft.network.InMemoryMessageRouter.runMessages;
import static org.kanelbullar.raft.server.RaftServer.Role.CANDIDATE;
import static org.kanelbullar.raft.server.RaftServer.Role.LEADER;

public class LeaderElectionTest {

    @Test
    public void testElection() {
        // Testing election using the configuration in Figure 6
        // Loop through each servers and have each trigger an election
        Network network = new InMemoryNetwork();
        int numNodes = 5;
        Map<String, RaftNode> nodes = network.createNodes(numNodes);

        {
            // election on node 0 - it should win all votes
            Map<String, RaftServer> servers = createServers(nodes);
            RaftServer serverUnderTest = servers.get("node-0");
            serverUnderTest.becomeCandidate();
            runMessages(network, nodes, servers.values().toArray(new RaftServer[0]));
            assertEquals(LEADER, serverUnderTest.role());
            assertEquals(Set.of("node-0", "node-1", "node-2", "node-3", "node-4"), serverUnderTest.votesReceived());
        }

        {
            // election on node 1 - it should lose the election, but get a vote from 3
            Map<String, RaftServer> servers = createServers(nodes);
            RaftServer serverUnderTest = servers.get("node-1");
            serverUnderTest.becomeCandidate();
            runMessages(network, nodes, servers.values().toArray(new RaftServer[0]));
            assertEquals(CANDIDATE, serverUnderTest.role());
            assertEquals(Set.of("node-1", "node-3"), serverUnderTest.votesReceived());
        }

        {
            // election on node 2 - it should win the election with votes from everyone
            Map<String, RaftServer> servers = createServers(nodes);
            RaftServer serverUnderTest = servers.get("node-2");
            serverUnderTest.becomeCandidate();
            runMessages(network, nodes, servers.values().toArray(new RaftServer[0]));
            assertEquals(LEADER, serverUnderTest.role());
            assertEquals(Set.of("node-0", "node-1", "node-2", "node-3", "node-4"), serverUnderTest.votesReceived());
        }

        {
            // election on node 3 - it should lose the election, and receive no votes (except from itself)
            Map<String, RaftServer> servers = createServers(nodes);
            RaftServer serverUnderTest = servers.get("node-3");
            serverUnderTest.becomeCandidate();
            runMessages(network, nodes, servers.values().toArray(new RaftServer[0]));
            assertEquals(CANDIDATE, serverUnderTest.role());
            assertEquals(Set.of("node-3"), serverUnderTest.votesReceived());
        }

        {
            // election on node 4 - it should win but only with votes from 1 and 3
            Map<String, RaftServer> servers = createServers(nodes);
            RaftServer serverUnderTest = servers.get("node-4");
            serverUnderTest.becomeCandidate();
            runMessages(network, nodes, servers.values().toArray(new RaftServer[0]));
            assertEquals(LEADER, serverUnderTest.role());
            assertEquals(Set.of("node-1", "node-3", "node-4"), serverUnderTest.votesReceived());
        }
    }

    private Map<String, RaftServer> createServers(Map<String, RaftNode> nodes) {
        Map<String, RaftServer> servers = new HashMap<>();
        for (RaftNode node : nodes.values()) {
            servers.put(node.name, new RaftServer(node));
        }

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

        for (RaftServer server : servers.values()) {
            server.setCurrentTerm(3);
            server.becomeFollower();
        }

        return servers;
    }

    @Test
    public void testTimeouts() {
        Network network = new InMemoryNetwork();
        int numNodes = 5;
        Map<String, RaftNode> nodes = network.createNodes(numNodes);
        RaftServer[] servers = new RaftServer[numNodes];
        int serverIdx = 0;
        for (RaftNode node : nodes.values()) {
            RaftServer raftServer = new RaftServer(node);
            servers[serverIdx++] = raftServer;
            raftServer.becomeFollower();
        }

        // time is ticking away
        IntStream.range(0, 1000).forEach(i -> {
            for (RaftServer server : servers) {
                server.handleClockTick();
            }
        });
        runMessages(network, nodes, servers);

        RaftServer leader = null;
        for (RaftServer server : servers) {
            if (server.role() == LEADER) {
                leader = server;
            }
        }
        assertNotNull(leader);

        System.out.println(" The leader is: " + leader);
    }

}
