package org.kanelbullar.raft.server.election;

import org.kanelbullar.raft.message.Message;

import java.util.Objects;

// this should be the payload in the message, but *sigh, time is short
public class RequestVoteResponse extends Message {

    // current term, for candidate to update itself
    private final boolean voteGranted;

    public RequestVoteResponse(String source, String dest, int term, boolean voteGranted) {
        super(source, dest, term);
        this.voteGranted = voteGranted;
    }

    public boolean voteGranted() {
        return voteGranted;
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
        RequestVoteResponse that = (RequestVoteResponse) o;
        return voteGranted == that.voteGranted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), voteGranted);
    }

    @Override
    public String toString() {
        return "RequestVoteResponse{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                ", source='" + source + '\'' +
                ", dest='" + dest + '\'' +
                '}';
    }
}
