package org.kanelbullar.raft.log;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RaftLogTest {

    @Test
    public void testAppendToEmptyLog() {
        RaftLog log = new RaftLog();
        LogEntry entry = new LogEntry(1, "y");
        assertTrue(log.appendEntries(-1, 1, List.of(entry)));
        assertEquals(log.length(), 1);
        assertEquals(log.get(0), entry);
    }

    @Test
    public void testAppendEmptyEntries() {
        RaftLog log = new RaftLog();
        assertTrue(log.appendEntries(-1, 1, List.of()));
        assertEquals(log.length(), 0);
        LogEntry entry = new LogEntry(1, "y");
        log.appendEntries(-1, 1, List.of(entry));
        assertTrue(log.appendEntries(0, 1, List.of()));
        assertFalse("should not allow append when the previous term doesn't match the last one in the log", log.appendEntries(1, 2,
                List.of()));
        assertFalse("should not allow append with gaps", log.appendEntries(3, 2, List.of()));
    }

    // modeling the test scenarios from the paper - Figure 7
    @Test
    public void testLeaderForTerm8AppendsAtIndex11() {
        RaftLog log = new RaftLog(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(4, "y"),
                new LogEntry(4, "y"),
                new LogEntry(5, "y"),
                new LogEntry(5, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y")
        );

        // ah the 1-based list in the paper. translate this pup to 0-based
        assertTrue(log.appendEntries(9, 6, new LogEntry(8, "y")));
        assertEquals(11, log.length());
    }

    //(a) False. Missing entry at index 9.
    @Test
    public void testScenarioA_False_Gaps() {
        RaftLog log = new RaftLog(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(4, "y"),
                new LogEntry(4, "y"),
                new LogEntry(5, "y"),
                new LogEntry(5, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y")
        );
        assertFalse("Missing entry at index 9", log.appendEntries(9, 6, new LogEntry(8, "y")));
    }

    // (b) False. Many missing entries.
    @Test
    public void testScenarioB_False_Gaps() {
        RaftLog log = new RaftLog(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(4, "y")
        );
        assertFalse("Many missing entries", log.appendEntries(9, 6, new LogEntry(8, "y")));

    }

    // (c) True. Entry already in position 11 is replaced.
    @Test
    public void testScenarioC_True_EntryIsReplaced() {
        RaftLog log = new RaftLog(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(4, "y"),
                new LogEntry(4, "y"),
                new LogEntry(5, "y"),
                new LogEntry(5, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y")
        );

        assertTrue("The entry already in position 10 must be replaced",
                log.appendEntries(9, 6, new LogEntry(8, "y")));
        assertEquals(11, log.length());
    }

    // (d) True. Entries at position 11,12 are replaced.
    @Test
    public void testScenarioD_True_EntriesAreReplaced() {
        RaftLog log = new RaftLog(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(4, "y"),
                new LogEntry(4, "y"),
                new LogEntry(5, "y"),
                new LogEntry(5, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y"),
                new LogEntry(6, "y"),
                new LogEntry(7, "y"),
                new LogEntry(7, "y")
        );

        assertTrue("The entries at position 11,12 are replaced",
                log.appendEntries(9, 6, new LogEntry(8, "y")));
        assertEquals(11, log.length());
    }

    // (e) False. Missing entries
    @Test
    public void testScenarioE_False_Gaps() {
        RaftLog log = new RaftLog(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(4, "y"),
                new LogEntry(4, "y"),
                new LogEntry(4, "y"),
                new LogEntry(4, "y")
        );
        assertFalse("Many missing entries", log.appendEntries(9, 6, new LogEntry(8, "y")));
    }

    // (f) False. Previous term mismatch.
    @Test
    public void testScenarioF_False_PrevTermMismatch() {
        RaftLog log = new RaftLog(
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(1, "y"),
                new LogEntry(2, "y"),
                new LogEntry(2, "y"),
                new LogEntry(2, "y"),
                new LogEntry(3, "y"),
                new LogEntry(3, "y"),
                new LogEntry(3, "y"),
                new LogEntry(3, "y")
        );

        assertFalse("The entry on position 9 has different term (3)",
                log.appendEntries(9, 6, new LogEntry(8, "y")));
    }

    @Test
    public void testAddSomeEntries() {
        RaftLog log = new RaftLog();
        assertTrue("appending to an empty log should always work",
                log.appendEntries(-1, -1, new LogEntry(10, "x")));

        // same term number, next position, this should work
        assertTrue(log.appendEntries(0, 10, new LogEntry(10, "y")));

        // previous term number doesn't match index 0 term number (10)
        assertFalse(log.appendEntries(0, 9, new LogEntry(10, "y")));

        // gaps not allowed
        assertFalse(log.appendEntries(2, 10, new LogEntry(10, "z")));
    }
}
