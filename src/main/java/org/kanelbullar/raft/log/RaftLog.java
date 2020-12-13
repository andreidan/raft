package org.kanelbullar.raft.log;

import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RaftLog {

    private List<LogEntry> log = new ArrayList<>();
    private int commitIndex = -1;

    public RaftLog() {
    }

    @TestOnly
    RaftLog(LogEntry... entries) {
        log.addAll(Arrays.asList(entries));
    }

    /**
     * Adds one or more entries to the log and returns a True/False value to indicate success.
     *
     * @param prevIndex    indicates the the position in this log _after which_ the entries go (and the size of the source_log-1).
     *                     positions in the log is 0-based
     * @param previousTerm specifies the term value of the log entry in the source log, immediately before the new entries that are being
     *                     added
     * @param entries      to be added to this log
     */
    public boolean appendEntries(int prevIndex, int previousTerm, List<LogEntry> entries) {
        if (prevIndex >= log.size()) {
            // the log has gaps
            return false;
        }

        if (prevIndex >= 0 && log.get(prevIndex).term != previousTerm) {
            // verify if the previous term of the sender matches the term of the entry stored _before_ the position we want to
            // start appending at
            return false;
        }

        int sourceLogLength = prevIndex + 1;
        if (isDestinationLogConsistentWithSource(log, sourceLogLength, entries) == false) {
            // truncate the destination log up until the `sourceLogLength`
            log = truncate(log, sourceLogLength);
        }

        // if there are some entries we don't have in our log
        if (sourceLogLength + entries.size() > log.size()) {
            // let's make this append idempotent:
            //  - some of the entries already exists in our log, so let's add only the ones that don't exist already
            for (int i = log.size() - sourceLogLength; i < entries.size(); i++) {
                log.add(entries.get(i));
            }
        }

        return true;
    }

    @TestOnly
    boolean appendEntries(int prevIndex, int previousTerm, LogEntry entry) {
        return appendEntries(prevIndex, previousTerm, List.of(entry));
    }

    private static List<LogEntry> truncate(List<LogEntry> log, int toIndex) {
        return new ArrayList<>(log.subList(0, toIndex));
    }

    private static boolean isDestinationLogConsistentWithSource(List<LogEntry> destinationLog, int sourceLogLength,
                                                                List<LogEntry> newEntries) {
        if (newEntries.size() > 0 && destinationLog.size() > sourceLogLength) {
            // destinationLog has more entries than the source log, so there's overlap
            if (destinationLog.get(sourceLogLength).term != newEntries.get(0).term) {
                // if the first "to append" entry and the corresponding entry in the destination log do not have the same term
                // the destination log has some invalid entries (maybe seen them from a different leader?)
                return false;
            }
        }
        return true;
    }

    public int length() {
        return log.size();
    }

    public LogEntry get(int index) {
        return log.get(index);
    }

    public List<LogEntry> getEntriesFrom(int index) {
        return new ArrayList<>(log.subList(index, log.size()));
    }

    public List<LogEntry> subLog(int from, int to) {
        return new ArrayList<>(log.subList(from, to));
    }

    @TestOnly
    public void addAll(LogEntry... entries) {
        log.addAll(Arrays.asList(entries));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RaftLog raftLog = (RaftLog) o;
        return Objects.equals(log, raftLog.log);
    }

    @Override
    public int hashCode() {
        return Objects.hash(log);
    }

    @Override
    public String toString() {
        return "RaftLog{" +
                "log=" + log +
                '}';
    }
}
