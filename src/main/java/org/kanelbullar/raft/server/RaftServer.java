package org.kanelbullar.raft.server;

import org.jetbrains.annotations.TestOnly;
import org.kanelbullar.raft.log.LogEntry;
import org.kanelbullar.raft.log.RaftLog;
import org.kanelbullar.raft.message.PoisonPill;
import org.kanelbullar.raft.network.RaftNode;
import org.kanelbullar.raft.server.election.RequestVoteMessage;
import org.kanelbullar.raft.server.election.RequestVoteResponse;
import org.kanelbullar.raft.server.replication.AppendEntriesRequest;
import org.kanelbullar.raft.server.replication.AppendEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RaftServer {

    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    public enum Role {
        FOLLOWER, CANDIDATE, LEADER
    }

    final RaftNode self;
    private Role role;
    final RaftLog log;
    // for each server keeps the index of the next entry _to send_
    private final Map<String, Integer> nextIndex;
    // for each server records the index of the highest entry known
    // to be replicated on that server. by looking at all the values
    // the server can decide the committed index, by sorting the list
    // in ascending order and taking the median
    // eg. -  matchIndex = [8, 5, 8, 2, 7]
    //      - sort [2, 5, 7, 8, 8]
    //         => committed index: 7

    private final Map<String, Integer> matchIndex;
    private int currentTerm;
    // highest log index known to be committed
    int commitIndex;
    // highest log index applied to the state machine (executed for client)
    int lastApplied;

    // number of acks needed to decide (the quorum number)
    private int minAcks;

    // name of the {@link RaftNode} this server voted for in the current term
    private String votedFor;
    private Set<String> votesReceived;

    // timing
    private int clock;
    private int heartbeatTimer;
    private int electionTimer;
    private int electionRandomnessUpperBound;
    private int deadline;

    RaftServer(RaftNode self) {
        // A node is a network endpoint.
        this.self = self;
        this.role = Role.FOLLOWER;
        this.log = new RaftLog();

        int numNodes = self.peers.size() + 1;
        this.minAcks = (numNodes + 1) / 2;

        this.currentTerm = 1;
        // paper has 0, as everything's 1-indexed
        this.commitIndex = -1;
        this.lastApplied = -1;

        // Follower state tracked by the leader
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();

        // Voting
        this.votedFor = "";
        this.votesReceived = new HashSet<>();

        // Timing
        this.heartbeatTimer = 10;
        this.electionTimer = 50;
        this.electionRandomnessUpperBound = 30;
        this.deadline = -1; // timer deadline
        this.clock = 0;

        logger.debug("RaftServer [{}]", self.name);
    }

    void resetElectionTimer() {
        clock = 0;
        deadline = electionTimer + new Random().nextInt(electionRandomnessUpperBound);
    }

    void resetHeartbeatTimer() {
        clock = 0;
        deadline = heartbeatTimer;
    }

    void becomeLeader() {
        logger.debug("[{}] became Leader", self.name);
        this.role = Role.LEADER;
        for (String peer : self.peers) {
            // upon becoming leader we don't know much so we assume each follower
            // looks exactly like us, meaning all followers have the same log as me
            nextIndex.put(peer, log.length());
            matchIndex.put(peer, -1);
        }
        // tracking myself too
        matchIndex.put(self.name, log.length());

        // Immediately upon becoming leader, send an AppendEntries to assert ourselves.
        sendAppendEntriesToAll();

        resetHeartbeatTimer();
    }

    void becomeCandidate() {
        logger.debug("[{}] became Candidate", self.name);
        role = Role.CANDIDATE;
        currentTerm++;
        // I vote for myself
        votedFor = self.name;
        votesReceived = new HashSet<>();
        votesReceived.add(self.name);

        int lastIndex = log.length() - 1;
        int lastTerm = lastIndex >= 0 ? log.get(lastIndex).term : -1;
        for (String peer : self.peers) {
            var voteRequest = new RequestVoteMessage(self.name, peer, currentTerm, lastIndex, lastTerm);
            self.send(voteRequest);
        }

        resetElectionTimer();
    }

    void becomeFollower() {
        logger.debug("[{}] Became Follower", self.name);
        this.role = Role.FOLLOWER;
        resetElectionTimer();
    }

    /**
     * Appends a new entry to own log and replicates the entry to peers.
     * This method is used by clients and should only be executed on the leader.
     */
    public void clientAppendEntry(String command) {
        if (role != Role.LEADER) {
            // TODO this will eventually forward the request to the current leader
            logger.error("client append entry must called on leader. current node is [{}]", self);
            return;
        }
        LogEntry entry = new LogEntry(currentTerm, command);
        int prevIndex = log.length() - 1;
        int previousTerm = prevIndex >= 0 ? log.get(prevIndex).term : -1;
        boolean success = log.appendEntries(prevIndex, previousTerm, List.of(entry));
        assert success : "leader should always be able to append to its own log";

        // assume the leader's matchIndex is the entire log (this needs to be present in order
        // to correctly calculate the median)
        matchIndex.put(self.name, log.length() - 1);

        for (String peerAddress : self.peers) {
            sendAppendEntriesToPeer(peerAddress);
        }
    }

    /**
     * Sends the corresponding {@link AppendEntriesRequest} to the specified peer.
     * This takes the corresponding {@link #nextIndex} value into account and sends that
     * entry to the peer.
     */
    private void sendAppendEntriesToPeer(String peer) {
        // Reminder: nextIndex tracks the log length on each follower
        int prevIndex = nextIndex.get(peer) - 1;
        int previousTerm = prevIndex >= 0 ? log.get(prevIndex).term : -1;

        List<LogEntry> entries = log.getEntriesFrom(prevIndex + 1);

        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(self.name, peer, currentTerm,
                prevIndex, previousTerm, commitIndex, entries);
        self.send(appendEntriesRequest);
    }

    private void sendAppendEntriesToAll() {
        // Send an AppendEntries message to all peers.
        // This is something that might be done on a new leader or for a heartbeat.
        for (String peer : self.peers) {
            sendAppendEntriesToPeer(peer);
        }
    }

    public void handleAppendEntriesRequest(AppendEntriesRequest request) {
        if (role == Role.LEADER) {
            logger.error("append entries must be handled by followers. current node is [{}]", self);
            return;
        }

        // if I am candidate in this term but someone else won the election for this term
        // the new leader will send `AppendEntries` requests upon winning
        if (role == Role.CANDIDATE) {
            becomeFollower();
        }

        logger.debug("[{}] handling append entries request [{}]", self.name, request);
        boolean success = log.appendEntries(request.prevIndex(), request.previousTerm(), request.entries());
        logger.debug("[{}] follower [{}]: appended entries? [{}]", self.name, self.name, success);
        int matchIndex = request.prevIndex() + request.entries().size();
        AppendEntriesResponse appendEntriesResponse =
                new AppendEntriesResponse(self.name, request.source(), currentTerm, success, matchIndex);
        self.send(appendEntriesResponse);

        // The leader always sends its commit index.
        int newCommitIndex = Math.min(log.length() - 1, request.leaderCommitIndex());
        if (newCommitIndex > commitIndex) {
            logger.debug("[{}] old commit index [{}], new commit index [{}]", self.name, commitIndex, newCommitIndex);
            commitIndex = newCommitIndex;
            if (commitIndex > lastApplied) {
                List<LogEntry> toApply = log.subLog(lastApplied + 1, commitIndex + 1);
                // TODO deliver these to the application (maybe with a callback?)
                logger.debug("Applying entries [{}]", toApply);
                lastApplied = commitIndex;
            }
        }
    }

    public void handleAppendEntriesResponse(AppendEntriesResponse response) {
        if (role != Role.LEADER) {
            logger.error("append entries response must be handled by the leader. current node is [{}]", self);
            return;
        }

        String peer = response.source();
        logger.debug("[{}] entries appended. follower [{}] successful? [{}]. received response [{}]", self.name, peer,
                response.success(), response);
        if (response.success()) {
            nextIndex.put(peer, response.matchIndex() + 1);
            matchIndex.put(peer, response.matchIndex());
            logger.debug("[{}] peer id: [{}], nextIndex [{}], matchIndex[{}]", self.name, peer, nextIndex.get(peer),
                    matchIndex.get(peer));

            // calculate the commit index, a bit cheeky, but cool trick here
            // we're sorting the matchIndex and setting the commitIndex to be the median value
            Integer[] sortedMatchIndex = matchIndex.values().toArray(new Integer[0]);
            Arrays.sort(sortedMatchIndex);
            int newCommitIndex = sortedMatchIndex[minAcks];

            // We used matchedIndex to calculate a possible new commit index (namely, how many
            // log entries have been replicated across a majority of machines)
            //
            // The term check below ensures we don't advance the commit unless it includes entries from my own term
            //
            // Figure 8: [...] a leader cannot determine commitment using log entries from older terms.
            // https://raft.github.io/raft.pdf
            //
            if (newCommitIndex > commitIndex && log.get(newCommitIndex).term == currentTerm) {
                logger.debug("[{}] old commit index [{}], new commit index [{}]", self.name, commitIndex, newCommitIndex);
                commitIndex = newCommitIndex;

                if (lastApplied < commitIndex) {
                    List<LogEntry> toApply = log.subLog(lastApplied + 1, commitIndex + 1);
                    // TODO deliver these to the application (maybe with a callback?)
                    logger.debug("Applying entries [{}]", toApply);
                    lastApplied = commitIndex;
                }
            }
        } else {
            // failed to append entries. backtrack
            if (nextIndex.get(peer) >= 0) {
                // reminder : nextIndex stores the log length on the follower
                // we reduce the perceived length of the follower here to retry appending entries
                // starting one index previous in the leader log than what we tried now
                int prevIndex = nextIndex.get(peer) - 1;
                nextIndex.put(peer, prevIndex);
            }
            logger.debug("[{}] backtracking for follower [{}]. new nextIndex is [{}]. resending entry to peer",
                    self.name, peer, nextIndex.get(peer));
            sendAppendEntriesToPeer(peer);
        }
    }

    public void handleRequestVoteMessage(RequestVoteMessage requestVoteMessage) {
        logger.debug("[{}] handle request vote message [{}]", self.name, requestVoteMessage);
        /*
         * 1. Reply false if term < currentTerm (§5.1)
         * 2. If votedFor is null or candidateId, and candidate’s log is at
         * least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
         */

        boolean voteGranted = true;

        if (votedFor.equals("") == false && votedFor.equals(requestVoteMessage.source()) == false) {
            // already voted
            voteGranted = false;
        } else {
            /*
             * Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
             * If the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs end
             * with the same term, then whichever log is longer is more up-to-date.
             */
            int myLastIndex = log.length() - 1;
            int myLastTerm = myLastIndex >= 0 ? log.get(myLastIndex).term : -1;

            if (myLastTerm > requestVoteMessage.lastTerm()) {
                voteGranted = false; // my log ends with a greater term
            } else if (myLastTerm == requestVoteMessage.lastTerm() && myLastIndex > requestVoteMessage.lastIndex()) {
                voteGranted = false; // same term but my log has more entries
            }
        }

        if (voteGranted) {
            // I vote for this candidate
            votedFor = requestVoteMessage.source();
        }

        RequestVoteResponse response = new RequestVoteResponse(
                self.name,
                requestVoteMessage.source(),
                currentTerm,
                voteGranted
        );
        self.send(response);
    }

    private void handleRequestVoteResponse(RequestVoteResponse response) {
        logger.debug("[{}] handle request vote response [{}]", self.name, response);
        if (role == Role.FOLLOWER) {
            return;
        }

        if (response.voteGranted()) {
            // record granted vote (even if the node has been elected already as it's very
            // useful for testing to verify the correct behaviour
            votesReceived.add(response.source());

            if (role == Role.CANDIDATE && votesReceived.size() >= minAcks) {
                becomeLeader();
            }
        }
    }

    public void deliverMessages() {
        while (true) {
            var msg = self.deliver(); // blocking
            logger.debug("[{}] delivering message [{}], [{}]", self.name, msg, self.incoming);

            if (msg == null) {
                // interrupted whilst waiting for a message to arrive
                continue;
            }

            // if we need to stop we stop, regardless of term (this is mostly for testing)
            if (msg instanceof PoisonPill) {
                logger.warn("[{}] You're poison, running through my veins, You're poison! [{}]", self.name, msg);
                break;
            }

            if (msg.term() > currentTerm) {
                currentTerm = msg.term();
                votedFor = ""; // clearing our "voted for" status as we haven't voted in this new term
                becomeFollower();
            }

            if (msg instanceof AppendEntriesRequest) {
                handleAppendEntriesRequest((AppendEntriesRequest) msg);
            } else if (msg instanceof AppendEntriesResponse) {
                handleAppendEntriesResponse((AppendEntriesResponse) msg);
            } else if (msg instanceof RequestVoteMessage) {
                handleRequestVoteMessage((RequestVoteMessage) msg);
            } else if (msg instanceof RequestVoteResponse) {
                handleRequestVoteResponse((RequestVoteResponse) msg);
            } else {
                logger.warn("[{}] received unknown message. dropping it [{}]", self.name, msg);
            }
        }
    }

    public void handleClockTick() {
        clock += 1;
        if (clock == deadline) {
            if (role == Role.LEADER) {
                // we need to send a heartbeat
                sendAppendEntriesToAll();
                clock = 0;
            } else {
                // election timeout occurred, let's become a candidate
                becomeCandidate();
                clock = 0;
            }
        }
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public Role role() {
        return role;
    }

    public Set<String> votesReceived() {
        return Collections.unmodifiableSet(votesReceived);
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    @TestOnly
    void setCurrentTerm(int newTerm) {
        this.currentTerm = newTerm;
    }

    @Override
    public String toString() {
        return "RaftServer{" +
                "self=" + self +
                ", role=" + role +
                ", log=" + log +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                ", currentTerm=" + currentTerm +
                ", commitIndex=" + commitIndex +
                ", lastApplied=" + lastApplied +
                ", minAcks=" + minAcks +
                ", votedFor='" + votedFor + '\'' +
                ", votesReceived=" + votesReceived +
                '}';
    }
}
