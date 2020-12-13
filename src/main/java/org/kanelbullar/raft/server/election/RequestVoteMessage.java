package org.kanelbullar.raft.server.election;

import org.kanelbullar.raft.message.Message;

import java.util.Objects;

// this should be the payload in the message, but *sigh, time is short
public class RequestVoteMessage extends Message {

    // index of candidate's last log entry
    private final int lastIndex;
    // term of candidate's last log entry
    private final int lastTerm;

    public RequestVoteMessage(String candidate, String dest, int term, int lastIndex, int lastTerm) {
        super(candidate, dest, term);
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
    }

    public int lastIndex() {
        return lastIndex;
    }

    public int lastTerm() {
        return lastTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RequestVoteMessage that = (RequestVoteMessage) o;
        return lastIndex == that.lastIndex &&
                lastTerm == that.lastTerm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lastIndex, lastTerm);
    }

    @Override
    public String toString() {
        return "RequestVoteRequest{" +
                "term=" + term +
                ", lastIndex=" + lastIndex +
                ", lastTerm=" + lastTerm +
                ", source='" + source + '\'' +
                ", dest='" + dest + '\'' +
                '}';
    }
}
