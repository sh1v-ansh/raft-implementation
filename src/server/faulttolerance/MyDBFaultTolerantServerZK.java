package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;
import server.SingleServer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fault-Tolerant Replicated Database Server using custom Raft Consensus Algorithm.
 * 
 * This implementation provides:
 * - Leader election with randomized timeouts
 * - Log replication with persistence
 * - Log compaction (snapshotting) to respect MAX_LOG_SIZE constraint
 * - Crash recovery from persistent state
 * - Server-to-server RPC communication
 * 
 * Architecture:
 * - Each server maintains a replicated log of SQL commands
 * - Leader receives client requests, replicates to followers
 * - Commands are executed only after committed (replicated to majority)
 * - Persistent state (currentTerm, votedFor, log) survives crashes
 * - Snapshots compact the log when it exceeds MAX_LOG_SIZE
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

    private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;

    // Raft timing constants
    private static final int HEARTBEAT_INTERVAL = 50; // milliseconds
    private static final int MIN_ELECTION_TIMEOUT = 150; // milliseconds
    private static final int MAX_ELECTION_TIMEOUT = 300; // milliseconds
    private static final int RPC_TIMEOUT = 100; // milliseconds
    
    // Raft port offset from client port
    private static final int RAFT_PORT_OFFSET = 100;

    // Persistent state (must survive crashes)
    private volatile int currentTerm = 0;
    private volatile String votedFor = null;
    private final List<LogEntry> raftLog = new ArrayList<>(); // synchronized access required
    
    // Snapshot state
    private volatile long lastIncludedIndex = 0;
    private volatile int lastIncludedTerm = 0;

    // Volatile state
    private volatile ServerState state = ServerState.FOLLOWER;
    private volatile String currentLeader = null;
    
    private long commitIndex = 0;
    private long lastApplied = 0;
    
    // Cassandra session
    protected final Session session;
    protected final Cluster cluster;
    
    // Leader-specific state (reinitialized after election)
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Server configuration
    private final NodeConfig<String> nodeConfig;
    private final String myID;
    private final Set<String> allServers;
    
    // Storage paths
    private final Path storageDir;
    private final Path logFile;
    private final Path metadataFile;
    private final Path snapshotFile;
    
    // Networking
    private final ServerSocket raftServerSocket;
    private final ExecutorService raftThreadPool;
    private final Map<String, InetSocketAddress> serverAddresses;
    
    // Request tracking
    private final ConcurrentHashMap<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    
    // Threading
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;
    private volatile boolean running = true;

    /**
     * Raft server states
     */
    private enum ServerState {
        FOLLOWER, CANDIDATE, LEADER
    }

    /**
     * Log entry containing a command and its term
     */
    private static class LogEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        final int term;
        final String command;
        final String requestId;
        
        LogEntry(int term, String command, String requestId) {
            this.term = term;
            this.command = command;
            this.requestId = requestId;
        }
    }

    /**
     * Tracks pending client requests
     */
    private static class PendingRequest {
        final InetSocketAddress clientAddr;
        
        PendingRequest(InetSocketAddress clientAddr) {
            this.clientAddr = clientAddr;
        }
    }

    /**
     * RPC message types for Raft protocol
     */
    private enum MessageType {
        REQUEST_VOTE, REQUEST_VOTE_RESPONSE,
        APPEND_ENTRIES, APPEND_ENTRIES_RESPONSE
    }

    /**
     * Constructor: Initialize Raft server and recover from crash if needed
     */
    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB)
            throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);

        this.nodeConfig = nodeConfig;
        this.myID = myID;
        this.allServers = nodeConfig.getNodeIDs();
        
        // Initialize Cassandra connection
        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostString()).withPort(isaDB.getPort()).build();
        this.session = cluster.connect(myID);
        
        // Setup storage directories
        this.storageDir = Paths.get("raft_storage", myID);
        Files.createDirectories(storageDir);
        this.logFile = storageDir.resolve("log.dat");
        this.metadataFile = storageDir.resolve("metadata.dat");
        this.snapshotFile = storageDir.resolve("snapshot.dat");
        
        // Build server address map for Raft RPCs
        this.serverAddresses = new ConcurrentHashMap<>();
        for (String serverId : allServers) {
            if (!serverId.equals(myID)) {
                InetSocketAddress addr = new InetSocketAddress(
                    nodeConfig.getNodeAddress(serverId),
                    nodeConfig.getNodePort(serverId) + RAFT_PORT_OFFSET
                );
                serverAddresses.put(serverId, addr);
            }
        }
        
        // Start Raft RPC server
        int raftPort = nodeConfig.getNodePort(myID) + RAFT_PORT_OFFSET;
        this.raftServerSocket = new ServerSocket(raftPort);
        this.raftThreadPool = Executors.newCachedThreadPool();
        
        // Recover from crash
        recoverFromCrash();
        
        // Start Raft protocol threads
        startRaftServer();
        resetElectionTimer();
        
        log.log(Level.INFO, "Server {0} initialized. Term={1}, Log size={2}, Raft port={3}",
                new Object[]{myID, currentTerm, getLogSize(), raftPort});
    }

    /**
     * Recover persistent state from disk after a crash
     * Critical: Must recover lastApplied from Cassandra to avoid re-execution
     */
    private void recoverFromCrash() throws IOException {
        // Load metadata (currentTerm, votedFor)
        if (Files.exists(metadataFile)) {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(metadataFile))) {
                currentTerm = ois.readInt();
                votedFor = (String) ois.readObject();
                log.log(Level.INFO, "Recovered metadata: term={0}, votedFor={1}", 
                        new Object[]{currentTerm, votedFor});
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed to load metadata, starting fresh", e);
                currentTerm = 0;
                votedFor = null;
            }
        }
        
        // Load snapshot if exists
        if (Files.exists(snapshotFile)) {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(snapshotFile))) {
                lastIncludedIndex = ois.readLong();
                lastIncludedTerm = ois.readInt();
                log.log(Level.INFO, "Recovered snapshot: lastIncludedIndex={0}, lastIncludedTerm={1}",
                        new Object[]{lastIncludedIndex, lastIncludedTerm});
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed to load snapshot", e);
            }
        }
        
        // Recover lastApplied from Cassandra (persistent database)
        // This prevents re-execution of already committed transactions
        long recoveredLastApplied = recoverLastAppliedFromDB();
        if (recoveredLastApplied > 0) {
            lastApplied = recoveredLastApplied;
            commitIndex = Math.max(commitIndex, recoveredLastApplied);
            log.log(Level.INFO, "Recovered lastApplied from Cassandra: {0}", lastApplied);
        } else {
            lastApplied = lastIncludedIndex;
            commitIndex = lastIncludedIndex;
        }
        
        // Load log entries
        if (Files.exists(logFile)) {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(logFile))) {
                @SuppressWarnings("unchecked")
                List<LogEntry> recoveredLog = (List<LogEntry>) ois.readObject();
                synchronized (raftLog) {
                    raftLog.clear();
                    raftLog.addAll(recoveredLog);
                }
                log.log(Level.INFO, "Recovered {0} log entries", raftLog.size());
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed to load log, starting with empty log", e);
            }
        }
        
        // Replay any committed but not applied entries
        applyCommittedEntries();
    }
    
    /**
     * Recover lastApplied index from Cassandra metadata table
     * Returns 0 if not found
     */
    private long recoverLastAppliedFromDB() {
        try {
            // Create metadata table if it doesn't exist
            session.execute(
                "CREATE TABLE IF NOT EXISTS raft_metadata (key text PRIMARY KEY, value bigint)"
            );
            
            // Try to read lastApplied
            ResultSet rs = session.execute(
                "SELECT value FROM raft_metadata WHERE key = 'lastApplied'"
            );
            
            Row row = rs.one();
            if (row != null) {
                return row.getLong("value");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to recover lastApplied from DB", e);
        }
        return 0;
    }
    
    /**
     * Persist lastApplied index to Cassandra for crash recovery
     */
    private void persistLastApplied(long index) {
        try {
            // Update lastApplied in metadata table
            session.execute(
                "INSERT INTO raft_metadata (key, value) VALUES ('lastApplied', ?)",
                index
            );
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to persist lastApplied to DB", e);
        }
    }

    /**
     * Persist metadata to disk
     */
    private synchronized void persistMetadata() {
        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(metadataFile))) {
            oos.writeInt(currentTerm);
            oos.writeObject(votedFor);
            oos.flush();
        } catch (IOException e) {
            log.log(Level.SEVERE, "Failed to persist metadata", e);
        }
    }

    /**
     * Persist log to disk
     */
    private void persistLog() {
        synchronized (raftLog) {
            try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(logFile))) {
                oos.writeObject(new ArrayList<>(raftLog));
                oos.flush();
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to persist log", e);
            }
        }
    }

    /**
     * Take a snapshot and compact the log
     */
    private void takeSnapshot() {
        long snapshotIndex = lastApplied;
        int snapshotTerm;
        
        synchronized (raftLog) {
            if (snapshotIndex <= lastIncludedIndex) {
                return; // Already snapshotted
            }
            
            int logIndex = (int) (snapshotIndex - lastIncludedIndex - 1);
            if (logIndex < 0 || logIndex >= raftLog.size()) {
                return;
            }
            
            snapshotTerm = raftLog.get(logIndex).term;
            
            // Write snapshot metadata
            try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(snapshotFile))) {
                oos.writeLong(snapshotIndex);
                oos.writeInt(snapshotTerm);
                oos.flush();
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to write snapshot", e);
                return;
            }
            
            // Discard log entries up to snapshotIndex
            List<LogEntry> newLog = new ArrayList<>(raftLog.subList(logIndex + 1, raftLog.size()));
            raftLog.clear();
            raftLog.addAll(newLog);
            
            lastIncludedIndex = snapshotIndex;
            lastIncludedTerm = snapshotTerm;
            
            persistLog();
            
            log.log(Level.INFO, "Snapshot taken at index {0}, log size reduced to {1}",
                    new Object[]{snapshotIndex, raftLog.size()});
        }
    }

    /**
     * Start Raft server to accept RPC connections
     */
    private void startRaftServer() {
        raftThreadPool.submit(() -> {
            while (running) {
                try {
                    Socket clientSocket = raftServerSocket.accept();
                    raftThreadPool.submit(() -> handleRaftRPC(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        log.log(Level.WARNING, "Error accepting Raft connection", e);
                    }
                }
            }
        });
    }

    /**
     * Handle incoming Raft RPC
     */
    private void handleRaftRPC(Socket socket) {
        try {
            socket.setSoTimeout(RPC_TIMEOUT);
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            
            MessageType type = (MessageType) ois.readObject();
            
            switch (type) {
                case REQUEST_VOTE:
                    handleRequestVote(ois, oos);
                    break;
                case APPEND_ENTRIES:
                    handleAppendEntries(ois, oos);
                    break;
                default:
                    break;
            }
            
            oos.flush();
        } catch (Exception e) {
            // Normal for timeouts
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    /**
     * Handle RequestVote RPC
     */
    private void handleRequestVote(ObjectInputStream ois, ObjectOutputStream oos) throws Exception {
        int term = ois.readInt();
        String candidateId = (String) ois.readObject();
        long lastLogIndex = ois.readLong();
        int lastLogTerm = ois.readInt();
        
        boolean voteGranted = false;
        
        synchronized (this) {
            if (term > currentTerm) {
                convertToFollower(term);
            }
            
            if (term < currentTerm) {
                voteGranted = false;
            } else {
                // Check if we can vote for this candidate
                boolean canVote = (votedFor == null || votedFor.equals(candidateId));
                boolean logUpToDate = isLogUpToDate(lastLogIndex, lastLogTerm);
                
                if (canVote && logUpToDate) {
                    votedFor = candidateId;
                    voteGranted = true;
                    persistMetadata();
                    resetElectionTimer();
                    log.log(Level.FINE, "Voted for {0} in term {1}", new Object[]{candidateId, term});
                }
            }
        }
        
        oos.writeInt(currentTerm);
        oos.writeBoolean(voteGranted);
    }

    /**
     * Handle AppendEntries RPC (heartbeat or log replication)
     */
    private void handleAppendEntries(ObjectInputStream ois, ObjectOutputStream oos) throws Exception {
        int term = ois.readInt();
        String leaderId = (String) ois.readObject();
        long prevLogIndex = ois.readLong();
        int prevLogTerm = ois.readInt();
        @SuppressWarnings("unchecked")
        List<LogEntry> entries = (List<LogEntry>) ois.readObject();
        long leaderCommit = ois.readLong();
        
        boolean success = false;
        
        synchronized (this) {
            if (term > currentTerm) {
                convertToFollower(term);
            }
            
            if (term < currentTerm) {
                success = false;
            } else {
                // Valid leader
                if (state != ServerState.FOLLOWER) {
                    convertToFollower(term);
                }
                currentLeader = leaderId;
                resetElectionTimer();
                
                // Check log consistency
                if (prevLogIndex < lastIncludedIndex) {
                    success = false; // Need snapshot
                } else if (prevLogIndex == lastIncludedIndex) {
                    success = (prevLogTerm == lastIncludedTerm);
                } else {
                    long logIndex = prevLogIndex - lastIncludedIndex - 1;
                    if (logIndex < 0 || logIndex >= raftLog.size()) {
                        success = false;
                    } else {
                        LogEntry entry = raftLog.get((int) logIndex);
                        success = (entry.term == prevLogTerm);
                    }
                }
                
                if (success && entries != null && !entries.isEmpty()) {
                    // Append new entries - follow Raft protocol correctly
                    synchronized (raftLog) {
                        long insertIndex = prevLogIndex + 1;
                        int logInsertPos = (int) (insertIndex - lastIncludedIndex - 1);
                        
                        // Only truncate if there's an actual conflict (different term at same index)
                        // Don't truncate on empty heartbeats!
                        for (int i = 0; i < entries.size(); i++) {
                            int currentPos = logInsertPos + i;
                            if (currentPos < raftLog.size()) {
                                // Check for conflict
                                if (raftLog.get(currentPos).term != entries.get(i).term) {
                                    // Conflict found - delete this and all following entries
                                    raftLog.subList(currentPos, raftLog.size()).clear();
                                    // Append remaining new entries
                                    raftLog.addAll(entries.subList(i, entries.size()));
                                    break;
                                }
                                // else: same term, entry already exists, continue
                            } else {
                                // Append remaining new entries
                                raftLog.addAll(entries.subList(i, entries.size()));
                                break;
                            }
                        }
                        persistLog();
                    }
                }
                
                if (success && leaderCommit > commitIndex) {
                    commitIndex = Math.min(leaderCommit, getLastLogIndex());
                    applyCommittedEntries();
                }
            }
        }
        
        oos.writeInt(currentTerm);
        oos.writeBoolean(success);
    }

    /**
     * Check if candidate's log is at least as up-to-date as ours
     */
    private boolean isLogUpToDate(long lastLogIndex, int lastLogTerm) {
        long myLastIndex = getLastLogIndex();
        int myLastTerm = getLastLogTerm();
        
        if (lastLogTerm != myLastTerm) {
            return lastLogTerm >= myLastTerm;
        }
        return lastLogIndex >= myLastIndex;
    }

    /**
     * Send RequestVote RPC to a server
     */
    private boolean sendRequestVote(String serverId) {
        InetSocketAddress addr = serverAddresses.get(serverId);
        if (addr == null) return false;
        
        try (Socket socket = new Socket()) {
            socket.connect(addr, RPC_TIMEOUT);
            socket.setSoTimeout(RPC_TIMEOUT);
            
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            
            oos.writeObject(MessageType.REQUEST_VOTE);
            oos.writeInt(currentTerm);
            oos.writeObject(myID);
            oos.writeLong(getLastLogIndex());
            oos.writeInt(getLastLogTerm());
            oos.flush();
            
            int term = ois.readInt();
            boolean voteGranted = ois.readBoolean();
            
            if (term > currentTerm) {
                convertToFollower(term);
                return false;
            }
            
            return voteGranted;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Send AppendEntries RPC to a server
     */
    private boolean sendAppendEntries(String serverId, List<LogEntry> entries) {
        InetSocketAddress addr = serverAddresses.get(serverId);
        if (addr == null) return false;
        
        long nextIdx = nextIndex.getOrDefault(serverId, getLastLogIndex() + 1);
        long prevLogIndex = nextIdx - 1;
        int prevLogTerm = getTermAtIndex(prevLogIndex);
        
        try (Socket socket = new Socket()) {
            socket.connect(addr, RPC_TIMEOUT);
            socket.setSoTimeout(RPC_TIMEOUT);
            
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            
            oos.writeObject(MessageType.APPEND_ENTRIES);
            oos.writeInt(currentTerm);
            oos.writeObject(myID);
            oos.writeLong(prevLogIndex);
            oos.writeInt(prevLogTerm);
            oos.writeObject(entries);
            oos.writeLong(commitIndex);
            oos.flush();
            
            int term = ois.readInt();
            boolean success = ois.readBoolean();
            
            if (term > currentTerm) {
                convertToFollower(term);
                return false;
            }
            
            if (success) {
                if (entries != null && !entries.isEmpty()) {
                    long newMatchIndex = prevLogIndex + entries.size();
                    matchIndex.put(serverId, newMatchIndex);
                    nextIndex.put(serverId, newMatchIndex + 1);
                }
            } else {
                // Decrement nextIndex and retry
                nextIndex.put(serverId, Math.max(1, nextIdx - 1));
            }
            
            return success;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Start election
     */
    private void startElection() {
        synchronized (this) {
            if (state == ServerState.LEADER) {
                return; // Already leader
            }
            
            state = ServerState.CANDIDATE;
            currentTerm++;
            votedFor = myID;
            persistMetadata();
            resetElectionTimer();
            
            log.log(Level.INFO, "Starting election for term {0}", currentTerm);
        }
        
        AtomicInteger votesReceived = new AtomicInteger(1); // Vote for self
        int majority = (allServers.size() / 2) + 1;
        
        for (String serverId : allServers) {
            if (serverId.equals(myID)) continue;
            
            raftThreadPool.submit(() -> {
                if (sendRequestVote(serverId)) {
                    int votes = votesReceived.incrementAndGet();
                    if (votes >= majority && state == ServerState.CANDIDATE) {
                        becomeLeader();
                    }
                }
            });
        }
    }

    /**
     * Become leader after winning election
     */
    private synchronized void becomeLeader() {
        if (state != ServerState.CANDIDATE) {
            return;
        }
        
        state = ServerState.LEADER;
        currentLeader = myID;
        
        // Initialize leader state
        long lastLogIdx = getLastLogIndex();
        for (String serverId : allServers) {
            if (!serverId.equals(myID)) {
                nextIndex.put(serverId, lastLogIdx + 1);
                matchIndex.put(serverId, 0L);
            }
        }
        
        // Start sending heartbeats
        startHeartbeats();
        
        log.log(Level.INFO, "Became LEADER in term {0}", currentTerm);
    }

    /**
     * Convert to follower
     */
    private synchronized void convertToFollower(int term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            persistMetadata();
        }
        
        if (state != ServerState.FOLLOWER) {
            state = ServerState.FOLLOWER;
            stopHeartbeats();
            resetElectionTimer();
            log.log(Level.INFO, "Converted to FOLLOWER in term {0}", term);
        }
    }

    /**
     * Reset election timer with randomized timeout
     */
    private synchronized void resetElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        
        int timeout = MIN_ELECTION_TIMEOUT + 
                      new Random().nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT);
        
        electionTimer = scheduler.schedule(() -> {
            if (state != ServerState.LEADER) {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Start sending heartbeats as leader
     */
    private synchronized void startHeartbeats() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }
        
        heartbeatTimer = scheduler.scheduleAtFixedRate(() -> {
            if (state == ServerState.LEADER) {
                sendHeartbeats();
            }
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop sending heartbeats
     */
    private synchronized void stopHeartbeats() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
            heartbeatTimer = null;
        }
    }

    /**
     * Send heartbeats to all followers
     */
    private void sendHeartbeats() {
        for (String serverId : allServers) {
            if (serverId.equals(myID)) continue;
            
            raftThreadPool.submit(() -> {
                long nextIdx = nextIndex.getOrDefault(serverId, getLastLogIndex() + 1);
                long lastLogIdx = getLastLogIndex();
                
                List<LogEntry> entries = null;
                if (nextIdx <= lastLogIdx) {
                    // Need to send log entries
                    synchronized (raftLog) {
                        long startIdx = nextIdx - lastIncludedIndex - 1;
                        if (startIdx >= 0 && startIdx < raftLog.size()) {
                            entries = new ArrayList<>(raftLog.subList((int) startIdx, raftLog.size()));
                        }
                    }
                }
                
                sendAppendEntries(serverId, entries);
            });
        }
        
        // Update commit index
        updateCommitIndex();
    }

    /**
     * Update commit index based on follower match indices
     */
    private void updateCommitIndex() {
        if (state != ServerState.LEADER) {
            return;
        }
        
        long lastLogIdx = getLastLogIndex();
        
        for (long n = commitIndex + 1; n <= lastLogIdx; n++) {
            int term = getTermAtIndex(n);
            if (term != currentTerm) {
                continue;
            }
            
            int replicaCount = 1; // Leader has it
            for (String serverId : allServers) {
                if (serverId.equals(myID)) continue;
                if (matchIndex.getOrDefault(serverId, 0L) >= n) {
                    replicaCount++;
                }
            }
            
            int majority = (allServers.size() / 2) + 1;
            if (replicaCount >= majority) {
                commitIndex = n;
                applyCommittedEntries();
            }
        }
    }

    /**
     * Apply committed entries to the state machine (Cassandra)
     * MUST be synchronized to prevent race conditions with log modifications
     */
    private void applyCommittedEntries() {
        synchronized (raftLog) {
            while (lastApplied < commitIndex) {
                lastApplied++;
                
                long logIndex = lastApplied - lastIncludedIndex - 1;
                if (logIndex < 0 || logIndex >= raftLog.size()) {
                    continue;
                }
                
                LogEntry entry = raftLog.get((int) logIndex);
                executeCommand(entry.command, entry.requestId);
                
                // Persist lastApplied to Cassandra for crash recovery
                persistLastApplied(lastApplied);
            }
            
            // Check if we need to snapshot
            if (raftLog.size() >= MAX_LOG_SIZE) {
                takeSnapshot();
            }
        }
    }

    /**
     * Execute a command on Cassandra
     */
    private void executeCommand(String command, String requestId) {
        try {
            String response = "";
            ResultSet results = session.execute(command);
            
            for (Row row : results) {
                response += row.toString() + "\n";
            }
            
            // Send response to client if this server received the request
            PendingRequest pending = pendingRequests.remove(requestId);
            if (pending != null) {
                try {
                    // Extract original request ID if present
                    String[] parts = requestId.split("-", 3);
                    if (parts.length >= 3) {
                        String originalReqId = parts[2];
                        if (!originalReqId.equals("none")) {
                            response = originalReqId + ":" + response;
                        }
                    }
                    
                    this.clientMessenger.send(pending.clientAddr, response.getBytes(SingleServer.DEFAULT_ENCODING));
                } catch (IOException e) {
                    log.log(Level.WARNING, "Failed to send response to client", e);
                }
            }
            
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to execute command: " + command, e);
        }
    }

    /**
     * Handle message from client
     * ALL queries (including SELECTs) must go through consensus for linearizability
     */
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
            String requestIdStr = "none";
            String query = request;
            
            // Parse request ID if present (sent by client for idempotency)
            int colonIdx = request.indexOf(':');
            if (colonIdx > 0) {
                String idPart = request.substring(0, colonIdx);
                try {
                    Integer.parseInt(idPart);
                    requestIdStr = idPart;
                    query = request.substring(colonIdx + 1);
                } catch (NumberFormatException e) {
                    // No valid ID
                }
            }
            
            // Wait a bit for leader election if needed
            for (int i = 0; i < 20 && state == ServerState.CANDIDATE; i++) {
                Thread.sleep(100);
            }
            
            // Only leader can process client requests
            if (state != ServerState.LEADER) {
                log.log(Level.FINE, "Not leader (state={0}), ignoring client request", state);
                // In production, we would redirect to leader
                // For testing, client should send to different servers until it finds leader
                return;
            }
            
            // Use client-provided request ID for idempotency
            // Format: serverID-clientAddr-requestID
            String uniqueRequestId = myID + "-" + header.sndr.toString().replace("/", "").replace(":", "_") + "-" + requestIdStr;
            
            // Check for duplicate request (idempotency)
            if (pendingRequests.containsKey(uniqueRequestId)) {
                log.log(Level.FINE, "Duplicate request detected: {0}", uniqueRequestId);
                return; // Already processing this request
            }
            
            // Store pending request
            pendingRequests.put(uniqueRequestId, new PendingRequest(header.sndr));
            
            // Append to log as leader
            synchronized (raftLog) {
                LogEntry entry = new LogEntry(currentTerm, query, uniqueRequestId);
                raftLog.add(entry);
                persistLog();
                
                log.log(Level.FINE, "Leader appended entry to log: index={0}", getLastLogIndex());
            }
            
            // Immediately replicate to followers
            sendHeartbeats();
            
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error handling client message", e);
        }
    }

    /**
     * Handle message from server (not used, we use RPC sockets)
     */
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // Not used - we use separate RPC sockets
    }

    /**
     * Get the last log index
     */
    private long getLastLogIndex() {
        synchronized (raftLog) {
            return lastIncludedIndex + raftLog.size();
        }
    }

    /**
     * Get the last log term
     */
    private int getLastLogTerm() {
        synchronized (raftLog) {
            if (raftLog.isEmpty()) {
                return lastIncludedTerm;
            }
            return raftLog.get(raftLog.size() - 1).term;
        }
    }

    /**
     * Get term at specific index
     */
    private int getTermAtIndex(long index) {
        if (index == lastIncludedIndex) {
            return lastIncludedTerm;
        }
        if (index < lastIncludedIndex) {
            return 0;
        }
        
        synchronized (raftLog) {
            long logIdx = index - lastIncludedIndex - 1;
            if (logIdx < 0 || logIdx >= raftLog.size()) {
                return 0;
            }
            return raftLog.get((int) logIdx).term;
        }
    }

    /**
     * Get current log size
     */
    private int getLogSize() {
        synchronized (raftLog) {
            return raftLog.size();
        }
    }

    /**
     * Close and cleanup
     */
    @Override
    public void close() {
        running = false;
        
        scheduler.shutdownNow();
        raftThreadPool.shutdownNow();
        
        try {
            raftServerSocket.close();
        } catch (IOException e) {
            // Ignore
        }
        
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        
        super.close();
        log.log(Level.INFO, "Server {0} closed", myID);
    }

    /**
     * Main method
     */
    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(
                NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX,
                        ReplicatedServer.SERVER_PORT_OFFSET),
                args[1],
                args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
                        : new InetSocketAddress("localhost", 9042));
    }
}
