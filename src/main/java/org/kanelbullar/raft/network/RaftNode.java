package org.kanelbullar.raft.network;

import org.jetbrains.annotations.Nullable;
import org.kanelbullar.raft.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    // used internally to uniquely identify the node
    public final String name;
    public List<String> peers;
    // public for testing purposes
    public final BlockingQueue<Message> incoming;
    public final BlockingQueue<Message> outgoing;

    public RaftNode(String name) {
        this(name, List.of());
    }

    RaftNode(String name, List<String> peers) {
        this.name = name;
        this.peers = peers;
        this.incoming = new LinkedBlockingQueue<>(1024);
        this.outgoing = new LinkedBlockingQueue<>(1024);
    }

    /**
     * Sends a message to another node in the Raft network.
     * - Does not wait for the message to be delivered
     * - If the destination crashed, the message can be thrown away
     */
    public void send(Message message) {
        logger.debug("[{}] sending msg [{}]", this.name, message);
        outgoing.offer(message);
    }

    /**
     * Receive a message sent to "me" from any other node in the Raft network.
     * This will queue it up in the incoming queue for later processing.
     */
    public void receive(Message message) {
        logger.debug("[{}] receiving msg [{}]", this.name, message);
        incoming.offer(message);
    }

    /**
     * Deliver the message from the incoming queue to the app/client
     * If there's nothing to deliver this blocks until there's an available entry.
     */
    @Nullable
    public Message deliver() {
        try {
            Message take = incoming.take();
            logger.debug("[{}] delivering [{}]", this.name, take);
            return take;
        } catch (InterruptedException e) {
            return null;
        }
    }

    /**
     * Similar to {@link #deliver()} but waits the specified timeout for an entry to
     * become available.
     *
     * @return
     */
    @Nullable
    public Message deliver(long timeout, TimeUnit unit) {
        try {
            return incoming.poll(timeout, unit);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RaftNode raftNode = (RaftNode) o;
        return Objects.equals(name, raftNode.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "RaftNode{" +
                "address='" + name + '\'' +
                ", peers=" + peers +
                ", incoming=" + incoming +
                ", outgoing=" + outgoing +
                '}';
    }
}
