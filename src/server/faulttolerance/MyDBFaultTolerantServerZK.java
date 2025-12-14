package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fault-tolerant replicated DB server using Apache Ratis (Raft).
 *
 * You write code in exactly two main places:
 * 1) handleMessageFromClient: receive client request and propose to Raft
 * 2) MyStateMachine.applyTransaction: apply committed log entries in order (and
 * reply)
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Closeable {

    public static final int MAX_LOG_SIZE = 400;
    private static final String RATIS_STORAGE_DIR = "ratis_storage";

    private final NodeConfig<String> nodeConfig;
    private final String myID;
    private final InetSocketAddress isaDB;

    private RaftServer raftServer;
    private RaftClient raftClient;
    private RaftGroup raftGroup;

    /** Tracks last applied Raft log index (monotonic). */
    private final AtomicLong lastAppliedIndex = new AtomicLong(0L);

    /**
     * Map from requestId -> client socket address.
     * Only the replica that received the client request will have the requestId
     * here,
     * so only that replica will reply when the entry commits.
     */
    private final ConcurrentHashMap<String, InetSocketAddress> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Optional dedupe window (if your clients retry and you need exactly-once
     * behavior).
     */
    private final Set<Long> executedMarkers = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    private final File snapshotDir;

    protected final Session session;
    protected final Cluster cluster;

    /** The Ratis state machine instance that Raft calls on commit. */
    private final MyStateMachine stateMachine;

    /**
     * Ratis StateMachine implementation kept in the same file (inner class).
     * Ratis calls applyTransaction(...) for committed entries, in log order.
     */
    private final class MyStateMachine extends BaseStateMachine {

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            // TODO (READ FROM RATIS):
            // - Extract log index from trx
            // - Extract command bytes from trx (the replicated request payload)

            // TODO (APPLY TO YOUR DB STATE):
            // - Apply command to Cassandra in this method (this is the "state machine")
            // - Update lastAppliedIndex
            // - Optionally update executedMarkers for dedupe (if you use it)

            // TODO (REPLY TO CLIENT IF THIS NODE ORIGINATED THE REQUEST):
            // - Parse requestId from command bytes (your serialized format must include it)
            // - InetSocketAddress client = pendingRequests.remove(requestId);
            // - If client != null, send response bytes back over your NIO messenger

            // TODO (OPTIONAL SNAPSHOT POLICY):
            // - If you do snapshots: check if you should snapshot, then trigger snapshot
            // logic

            return CompletableFuture.completedFuture(Message.EMPTY);
        }

        @Override
        public long takeSnapshot() throws IOException {
            // TODO (ONLY IF YOU IMPLEMENT SNAPSHOTS):
            // - Persist a snapshot to snapshotDir
            // - Return the snapshot index (typically last applied index)
            return getLastAppliedTermIndex().getIndex();
        }
    }

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB)
            throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB,
                myID);

        this.nodeConfig = nodeConfig;
        this.myID = myID;
        this.isaDB = isaDB;

        this.stateMachine = new MyStateMachine();

        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostString()).build();
        this.session = cluster.connect(myID);

        this.snapshotDir = new File(RATIS_STORAGE_DIR + File.separator + myID);

        // TODO (RATIS BOOTSTRAP):
        // - Build raftGroup from nodeConfig (all replica ids + their raft
        // addresses/ports)
        // - Build raftClient for proposing entries
        // - Build and start raftServer with .setStateMachine(stateMachine)

        // TODO (RECOVERY):
        // - If you use snapshots, ensure state machine restore happens correctly
        // - If you keep your own snapshot files, load them / ensure Ratis sees them
        recoverFromCrash();
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // TODO (CLIENT RECEIVE PATH):
        // - Parse or generate a requestId
        // - Store pendingRequests.put(requestId, header.sndr)
        // - Ensure the command you send to Raft includes requestId (so applyTransaction
        // can find it)
        // - Propose to Raft via raftClient (leader will replicate/order it)
    }

    private void recoverFromCrash() {
        // TODO (ONLY IF YOU MANAGE SNAPSHOT/STATE OUTSIDE RATIS):
        // - Load latest snapshot/checkpoint into Cassandra and set lastAppliedIndex
        // - Otherwise, rely on Ratis StateMachine restore/snapshot mechanism
    }

    private boolean shouldCheckpoint(long lastApplied) {
        // TODO (OPTIONAL POLICY):
        // - return true when you want to snapshot (e.g., every K commits or when log is
        // large)
        return false;
    }

    private void checkpoint(long lastIncludedIndex) {
        // TODO (OPTIONAL SNAPSHOT IMPLEMENTATION):
        // - Persist durable snapshot and metadata
        // - Coordinate with Ratis snapshot/takeSnapshot if needed
    }

    @Override
    public void close() {
        // TODO:
        // - Stop raftClient and raftServer if started
        // - Close Cassandra session/cluster if you want clean shutdown
        super.close();
    }

    public static void main(String[] args) throws IOException {
        NodeConfig<String> nc = NodeConfigUtils.getNodeConfigFromFile(
                args[0],
                ReplicatedServer.SERVER_PREFIX,
                ReplicatedServer.SERVER_PORT_OFFSET);

        new MyDBFaultTolerantServerZK(
                nc,
                args[1],
                args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
                        : new InetSocketAddress("localhost", 9042));
    }
}
