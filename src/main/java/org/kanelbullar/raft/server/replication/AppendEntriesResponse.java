package org.kanelbullar.raft.server.replication;

import org.kanelbullar.raft.message.Message;

import java.util.Objects;

// this should be the payload in the message, but *sigh, time is short
public class AppendEntriesResponse extends Message {

    private final int term;
    private final boolean success;
    private final int matchIndex;

    public AppendEntriesResponse(String sourceName, String destination, int term, boolean success, int matchIndex) {
        super(sourceName, destination, term);
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public int term() {
        return term;
    }

    public boolean success() {
        return success;
    }

    public int matchIndex() {
        return matchIndex;
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
        AppendEntriesResponse that = (AppendEntriesResponse) o;
        return term == that.term &&
                success == that.success &&
                matchIndex == that.matchIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term, success, matchIndex);
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "sourceName='" + source + '\'' +
                ", destName='" + dest + '\'' +
                ", term=" + term +
                ", success=" + success +
                ", matchIndex=" + matchIndex +
                '}';
    }
}
