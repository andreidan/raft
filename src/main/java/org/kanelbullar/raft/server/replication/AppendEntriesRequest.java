package org.kanelbullar.raft.server.replication;

import org.kanelbullar.raft.log.LogEntry;
import org.kanelbullar.raft.message.Message;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

// this should be the payload in the message, but *sigh, time is short
public class AppendEntriesRequest extends Message {

    private final int term;
    private final int prevIndex;
    private final int previousTerm;
    private final List<LogEntry> entries;
    private final int leaderCommitIndex;

    public AppendEntriesRequest(String source, String dest, int term, int prevIndex, int previousTerm,
                                int leaderCommitIndex, List<LogEntry> entries) {
        super(source, dest, term);
        this.term = term;
        this.prevIndex = prevIndex;
        this.previousTerm = previousTerm;
        this.leaderCommitIndex = leaderCommitIndex;
        this.entries = entries;
    }

    public int prevIndex() {
        return prevIndex;
    }

    public int previousTerm() {
        return previousTerm;
    }

    public int leaderCommitIndex() {
        return leaderCommitIndex;
    }

    public List<LogEntry> entries() {
        return Collections.unmodifiableList(entries);
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
        AppendEntriesRequest that = (AppendEntriesRequest) o;
        return term == that.term &&
                prevIndex == that.prevIndex &&
                previousTerm == that.previousTerm &&
                leaderCommitIndex == that.leaderCommitIndex &&
                Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term, prevIndex, previousTerm, entries, leaderCommitIndex);
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
                "sourceName='" + source + '\'' +
                ", destName='" + dest + '\'' +
                ", term=" + term +
                ", prevIndex=" + prevIndex +
                ", previousTerm=" + previousTerm +
                ", leaderCommitIndex=" + leaderCommitIndex +
                ", entries=" + entries +
                '}';
    }
}
