package org.kanelbullar.raft.server;//package org.kanelbullar.raft.server;

import org.junit.Test;
import org.kanelbullar.raft.log.LogEntry;
import org.kanelbullar.raft.network.InMemoryNetwork;
import org.kanelbullar.raft.network.Network;
import org.kanelbullar.raft.network.RaftNode;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.kanelbullar.raft.network.InMemoryMessageRouter.runMessages;

public class InMemoryLogReplicationTest {

    @Test
    public void testLogReplication() {
        Network network = new InMemoryNetwork();
        int numNodes = 5;
        RaftServer[] servers = new RaftServer[numNodes];
        Map<String, RaftNode> nodes = network.createNodes(numNodes);

        int index = 0;
        for (RaftNode node : nodes.values()) {
            servers[index++] = new RaftServer(node);
        }

        servers[0].becomeLeader();
        servers[0].clientAppendEntry("testing");

        runMessages(network, nodes, servers);

        for (RaftServer server : servers) {
            assertEquals(servers[0].log, server.log);
            assertEquals(1, server.log.get(0).term);
            assertEquals("testing", server.log.get(0).command);
        }
    }

    @Test
    public void testLogReplicationFillsInGaps() {
        Network network = new InMemoryNetwork();
        int numNodes = 2;
        RaftServer[] servers = new RaftServer[numNodes];
        Map<String, RaftNode> nodes = network.createNodes(numNodes);

        int index = 0;
        for (RaftNode node : nodes.values()) {
            servers[index++] = new RaftServer(node);
        }

        RaftServer leader = servers[0];
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

        RaftServer follower = servers[1];
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

        runMessages(network, nodes, servers);

        assertEquals("the leader log must be replicated to all servers", leader.log, follower.log);
    }

    @Test
    public void testReplicationBacktrack_ScenarioSlide137() {
        Network network = new InMemoryNetwork();
        int numNodes = 5;
        RaftServer[] servers = new RaftServer[numNodes];
        Map<String, RaftNode> nodes = network.createNodes(numNodes);

        int index = 0;
        for (RaftNode node : nodes.values()) {
            servers[index++] = new RaftServer(node);
        }

        servers[0].log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0"),
                new LogEntry(3, "y<7"),
                new LogEntry(3, "x<5"),
                new LogEntry(3, "x<4")

        );

        servers[1].log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0")
        );

        servers[2].log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0"),
                new LogEntry(3, "y<7"),
                new LogEntry(3, "x<5"),
                new LogEntry(3, "x<4")
        );

        servers[3].log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1")
        );

        servers[4].log.addAll(
                new LogEntry(1, "x<3"),
                new LogEntry(1, "y<1"),
                new LogEntry(2, "x<2"),
                new LogEntry(3, "x<0"),
                new LogEntry(3, "y<7"),
                new LogEntry(3, "x<5")
        );

        servers[2].setCurrentTerm(4);
        servers[2].becomeLeader();
        servers[2].clientAppendEntry("y<7");

        runMessages(network, nodes, servers);

        for (RaftServer server : servers) {
            assertEquals("the leader log must be replicated to all servers, but failed on:" + server.self.name,
                    servers[2].log, server.log);
            assertEquals(4, server.log.get(server.log.length() - 1).term);
        }

    }
}
