package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;

import server.ReplicatedServer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;
import server.SingleServer;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.client.RaftClient;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.UUID;

/**
 * Fault-tolerant replicated DB server using Apache Ratis (Raft).
 *
 * Template only: method signatures + state-management scaffolding.
 * DO NOT add coordination anywhere else; keep all FT state management here.
 *
 * Intended architecture (high-level):
 * - Clients send requests to any replica
 * - Replica submits request to Raft log
 * - On commit, the state machine applies request in log order
 * - Checkpoint/snapshot bounds retained log size
 * - Recovery restores snapshot + replays committed log entries
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Closeable {


    public static final int MAX_LOG_SIZE = 400;

    /** Ratis storage dir base (you can choose a better location). */
    private static final String RATIS_STORAGE_DIR = "ratis_storage";

    private final NodeConfig<String> nodeConfig;
    private final String myID;
    private final InetSocketAddress isaDB;


    // TODO: replace Object with actual Ratis types:
    private org.apache.ratis.server.RaftServer raftServer;
    private org.apache.ratis.client.RaftClient raftClient;
    private org.apache.ratis.protocol.RaftGroup raftGroup;

    /* ******************************
     * Replication / ordering state
     * ******************************/

    /** Monotonically increasing "last applied" (e.g., Raft log index). */
    private final AtomicLong lastAppliedIndex = new AtomicLong(0L);

    /**
     * Pending client requests waiting for commit.
     * Key could be requestId / logIndex / clientProvidedId depending on design.
     */
    private final ConcurrentHashMap<String, InetSocketAddress> pendingRequests =
            new ConcurrentHashMap<>();

    /**
     * Track executed indices / ids if needed to prevent duplicates after retries.
     * (You can tune memory usage to respect MAX_LOG_SIZE constraint.)
     */
    private final Set<Long> executedMarkers =
            Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    /* ******************************
     * Snapshot / checkpoint state
     * ******************************/

    /** Directory for snapshots/checkpoints for this replica. */
    private final File snapshotDir;

    protected final Session session;
	protected final Cluster cluster;

    /**
     * @param nodeConfig Server name/address configuration information read from conf/servers.properties.
     * @param myID       The name of the keyspace to connect to, also the server id.
     * @param isaDB      The socket address of the backend datastore (Cassandra) instance.
     */
    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB)
            throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB,
                myID);

        this.nodeConfig = nodeConfig;
        this.myID = myID;
        this.isaDB = isaDB;

        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostString()).build();
		this.session = cluster.connect(myID);

        this.snapshotDir = new File(RATIS_STORAGE_DIR + File.separator + myID);

        // Template lifecycle steps:
        // 1) build raft group + config
        // 2) start raft server
        // 3) recover from snapshot/log if needed
        // 4) begin serving clients

        // TODO: build raftGroup, properties, storage dirs, state machine, etc.
        // raftGroup = buildRaftGroup();
        // raftClient = buildRaftClient();

        // TODO: construct and start raftServer using raftGroup + storage + stateMachine
        // raftServer = ...
        
        recoverFromCrash();
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // TODO: parse request bytes, create requestId, enqueue pending, submit to Raft.
        // submitToRaft(bytes, header.sndr);
    }

    private Object buildRaftGroup() {
        // TODO: build RaftGroup from nodeConfig (ids + addresses)
        return null;
    }

    private Object buildRaftClient() {
        // TODO: create RaftClient pointed at raftGroup
        return null;
    }

    /* =========================================================
     * Raft submission + commit callbacks
     * ========================================================= */

    /**
     * Submit a client request to Raft.
     * @param requestBytes the original client request bytes
     * @param clientAddr   the client socket address (for replying after commit)
     */
    private void submitToRaft(byte[] requestBytes, InetSocketAddress clientAddr) {
        // TODO: send requestBytes via raftClient, capture returned log index / requestId
        // TODO: map requestId -> clientAddr in pendingRequests
    }

    /**
     * Apply a committed log entry to the DB state machine.
     * In Ratis, this is typically inside a StateMachine.applyTransaction(...).
     *
     * @param logIndex the committed log index
     * @param commandBytes the replicated command
     * @return application response bytes to send back (or store for read-only clients)
     */
    private byte[] applyCommittedEntry(long logIndex, byte[] commandBytes) {
        // TODO: execute request on Cassandra (same logic as consistency server)
        // TODO: update lastAppliedIndex, executedMarkers
        // TODO: trigger checkpointing policy
        return null;
    }

    /**
     * Called when a client-visible response can be returned after commit+apply.
     *
     * @param requestId your chosen request identifier
     * @param responseBytes bytes to send back to the client
     */
    private void replyToClientAfterCommit(String requestId, byte[] responseBytes) {
        // TODO: find client in pendingRequests and send response over NIO messenger
        // TODO: remove pending entry
    }

    /* =========================================================
     * Crash recovery
     * ========================================================= */

    /**
     * Recover application state after crash/restart.
     * Typically:
     * 1) load latest snapshot/checkpoint
     * 2) replay committed log entries after snapshot index
     */
    private void recoverFromCrash() {
        // TODO: load snapshot from snapshotDir, restore lastAppliedIndex and DB state
        // TODO: ask Raft for committed entries after snapshot index and applyCommittedEntry(...)
    }

    /* =========================================================
     * Checkpointing / snapshotting
     * ========================================================= */

    /**
     * Decide if we should checkpoint/snapshot now (e.g., after every K commits).
     */
    private boolean shouldCheckpoint(long lastApplied) {
        // TODO: implement a policy that respects MAX_LOG_SIZE
        return false;
    }

    /**
     * Create a durable checkpoint/snapshot of application state.
     *
     * In Ratis, this generally maps to StateMachine.takeSnapshot().
     *
     * @param lastIncludedIndex snapshot index
     */
    private void checkpoint(long lastIncludedIndex) {
        // TODO: persist snapshot + metadata (lastIncludedIndex, any request dedupe window)
        // TODO: ask Raft to trim logs if applicable
    }

    // State manager methods

    public String getMyID() {
        return this.myID;
    }

    public long getLastAppliedIndex() {
        return this.lastAppliedIndex.get();
    }

    protected void setLastAppliedIndex(long idx) {
        this.lastAppliedIndex.set(idx);
    }

    public Map<String, InetSocketAddress> getPendingRequestsView() {
        return Collections.unmodifiableMap(this.pendingRequests);
    }

    protected void addPendingRequest(String requestId, InetSocketAddress clientAddr) {
        this.pendingRequests.put(requestId, clientAddr);
    }

    protected InetSocketAddress removePendingRequest(String requestId) {
        return this.pendingRequests.remove(requestId);
    }

    protected boolean isExecutedMarkerPresent(long marker) {
        return this.executedMarkers.contains(marker);
    }

    protected void addExecutedMarker(long marker) {
        this.executedMarkers.add(marker);
    }

    protected void clearExecutedMarkers() {
        this.executedMarkers.clear();
    }

    @Override
    public void close() {
        // TODO: shutdown raftClient, raftServer, any executors, then close superclass messenger
        super.close();
    }

    public static void main(String[] args) throws IOException {
       
            NodeConfig<String> nc = NodeConfigUtils.getNodeConfigFromFile(
                    args[0],
                    ReplicatedServer.SERVER_PREFIX,
                    ReplicatedServer.SERVER_PORT_OFFSET
            );

 
            new MyDBFaultTolerantServerZK(nc, args[1], args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
						: new InetSocketAddress("localhost", 9042));
            
         
    }

}
