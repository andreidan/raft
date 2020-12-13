package org.kanelbullar.raft.log;

import java.io.Serializable;
import java.util.Objects;

public class LogEntry implements Serializable {

    public final int term;
    public final String command;

    public LogEntry(int term, String command) {
        this.term = term;
        this.command = command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                Objects.equals(command, logEntry.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, command);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", command='" + command + '\'' +
                '}';
    }
}
