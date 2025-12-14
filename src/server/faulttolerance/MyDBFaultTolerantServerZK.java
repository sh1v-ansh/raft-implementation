package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.RaftServer;
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
 * ============================
 * You implement your system in TWO main code paths:
 * ============================
 *
 * (A) CLIENT PATH (network thread):
 * handleMessageFromClient(...)
 * - receive client bytes
 * - create a requestId
 * - remember who to reply to (pendingRequests.put(requestId, clientAddr))
 * - send a command into Raft via raftClient (this appends to Raft log)
 *
 * (B) REPLICA PATH (Ratis commit/apply thread):
 * MyStateMachine.applyTransaction(...)
 * - Ratis calls this when a log entry is COMMITTED (ordered + durable)
 * - apply the command to Cassandra (this is the "state machine apply")
 * - update local tracking (lastAppliedIndex, optional dedupe markers)
 * - reply to client ONLY if this node originated that request
 *
 * ============================
 * Crash recovery (IMPORTANT):
 * ============================
 * You do NOT write a separate recoverFromCrash().
 *
 * Recovery happens automatically when you start raftServer:
 * - Ratis loads its stored state from RATIS_STORAGE_DIR/<myID>
 * - Ratis loads the latest SNAPSHOT (if snapshots exist)
 * - Ratis replays committed log entries AFTER that snapshot by calling
 * applyTransaction(...)
 *
 * So: your recovery logic is implicitly your applyTransaction + snapshot
 * integration.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Closeable {

    /** Assignment requirement: bounded logs. */
    public static final int MAX_LOG_SIZE = 400;

    /**
     * Directory where Ratis stores its Raft metadata, log segments, and snapshots.
     */
    private static final String RATIS_STORAGE_DIR = "ratis_storage";

    private final NodeConfig<String> nodeConfig;
    private final String myID;
    private final InetSocketAddress isaDB;

    /**
     * Ratis objects:
     * - raftGroup: list of peers + groupId
     * - raftClient: used to propose commands (append to Raft log)
     * - raftServer: local Raft server that participates in consensus
     */
    private RaftServer raftServer;
    private RaftClient raftClient;
    private RaftGroup raftGroup;

    /**
     * Local tracking: last Raft log index applied on THIS node.
     * Not strictly required by Ratis, but useful for debugging and snapshot index.
     * During crash recovery, this value will be reconstructed as Ratis replays log
     * entries.
     */
    private final AtomicLong lastAppliedIndex = new AtomicLong(0L);

    /**
     * requestId -> client address
     *
     * This is how we ensure "only the node that received the request replies".
     * - handleMessageFromClient stores it
     * - applyTransaction removes it and replies IF present
     *
     * NOTE: after a crash, pendingRequests is lost; that's ok:
     * - client will retry
     * - dedupe markers (if you implement) can prevent double-apply
     */
    private final ConcurrentHashMap<String, InetSocketAddress> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Optional: used only if you need "exactly-once" semantics under client
     * retries.
     * If you don't need it, remove this completely.
     */
    private final Set<Long> executedMarkers = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    /**
     * Optional local dir you can use for *your own* metadata/debug.
     * Ratis also has its own storage directory (configured in RaftProperties).
     */
    private final File snapshotDir;

    /** Cassandra session for applying commands. */
    protected final Session session;
    protected final Cluster cluster;

    /** The StateMachine instance that Ratis calls. */
    private final MyStateMachine stateMachine;

    /**
     * ============================
     * Ratis StateMachine (INNER CLASS)
     * ============================
     *
     * Why inner class?
     * - keeps everything in one file
     * - can access outer fields directly (session, pendingRequests,
     * lastAppliedIndex, send/return path)
     *
     * Ratis uses this as follows:
     * - On normal operation: when an entry commits, calls applyTransaction(...) in
     * order.
     * - On restart: replays committed entries by calling applyTransaction(...)
     * again.
     *
     * The two additional methods you asked for:
     * - initialize(...): called once when the state machine is being attached to
     * the raft server/storage.
     * - getStateMachineStorage(): tells Ratis where/how this state machine stores
     * snapshot metadata.
     */
    private final class MyStateMachine extends BaseStateMachine {

        /**
         * Storage handle for snapshots and snapshot metadata that Ratis understands.
         *
         * If you implement bounded logs via snapshots, Ratis needs a
         * StateMachineStorage
         * so it can discover/install the latest snapshot and compact old logs safely.
         */
        private final org.apache.ratis.statemachine.impl.SimpleStateMachineStorage smStorage = new org.apache.ratis.statemachine.impl.SimpleStateMachineStorage();

        /**
         * Tells Ratis what storage object this StateMachine uses.
         *
         * Ratis uses this to:
         * - find the latest snapshot
         * - install snapshots
         * - coordinate log compaction safely (bounded logs)
         */
        @Override
        public org.apache.ratis.statemachine.StateMachineStorage getStateMachineStorage() {
            // TODO:
            // - If you use SimpleStateMachineStorage, returning it is usually enough.
            // - If you want custom snapshot layout, return your own StateMachineStorage
            // impl.
            return smStorage;
        }

        /**
         * Called by Ratis during startup to initialize the StateMachine with Raft
         * context + storage.
         *
         * This is where you "connect" your state machine to the Raft storage directory,
         * so
         * snapshots are stored/loaded correctly.
         *
         * Ratis crash recovery relies on:
         * - discovering/reading snapshot metadata via StateMachineStorage
         * - replaying committed entries via applyTransaction(...)
         */
        @Override
        public void initialize(org.apache.ratis.server.RaftServer server,
                org.apache.ratis.protocol.RaftGroupId groupId,
                org.apache.ratis.server.storage.RaftStorage raftStorage) throws IOException {
            // TODO:
            // 1) Initialize state machine storage against this RaftStorage:
            // smStorage.init(raftStorage);
            //
            // 2) Optionally load snapshot metadata here if you need it:
            // - smStorage.getLatestSnapshot()
            // - restore any state-machine metadata (e.g., lastAppliedIndex marker)
            //
            // 3) Do NOT replay the log yourself.
            // Ratis will replay committed entries by calling applyTransaction(...).

            super.initialize(server, groupId, raftStorage);
        }

        /**
         * Called by Ratis when a log entry is committed (decided + durable).
         * This method is the ONLY correct place to mutate replicated state.
         *
         * Runs on Ratis apply thread(s), not your NIO networking thread.
         */
        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            // ========= 1) Extract metadata and payload from Ratis =========
            // TODO:
            // - long logIndex = trx.getLogEntry().getIndex();
            // - byte[] commandBytes =
            // trx.getStateMachineLogEntry().getLogData().toByteArray();
            //
            // commandBytes must contain everything needed to apply the operation:
            // - requestId (so we can reply)
            // - operation type + arguments (e.g., put/get/table/key/value)
            //
            // IMPORTANT: This method runs on every replica for the same committed entries.

            // ========= 2) Apply to Cassandra (replicated state machine step) =========
            // TODO:
            // - Decode commandBytes
            // - Execute the operation on Cassandra using `session`
            // - Build the response bytes that the client expects
            //
            // This is where your "ZK apply logic" conceptually moves to.

            // ========= 3) Update local tracking =========
            // TODO:
            // - lastAppliedIndex.set(logIndex);
            // - optional: executedMarkers.add(<some marker>) if you implement dedupe

            // ========= 4) Reply only if THIS node originated request =========
            // TODO:
            // - Parse requestId from commandBytes
            // - InetSocketAddress clientAddr = pendingRequests.remove(requestId);
            // - If clientAddr != null:
            // send response bytes back to that client using your NIO messenger
            //
            // All other replicas will not have pendingRequests entry -> they do nothing.

            // ========= 5) Snapshot/log bounding =========
            // For bounded logs, you implement takeSnapshot() and configure snapshot policy.
            // You typically do NOT trigger snapshots manually here unless required.

            return CompletableFuture.completedFuture(Message.EMPTY);
        }

        /**
         * Snapshot hook: Ratis calls this to compact logs (bounded logs requirement).
         *
         * Think: "checkpoint" == "snapshot".
         *
         * What you must do:
         * - Ensure state up to some index is durable and can be recovered.
         * - Return the snapshot index so Ratis can delete older log entries.
         *
         * Important nuance for Cassandra-backed state:
         * - Cassandra is already durable, so your snapshot may be "logical":
         * - You may only need to return an index
         * - Optionally write minimal metadata (like lastAppliedIndex) to snapshotDir
         *
         * But: bounded logs require that this method is correct.
         */
        @Override
        public long takeSnapshot() throws IOException {
            // TODO (MINIMUM):
            // - Decide snapshotIndex (often: lastAppliedIndex.get() OR
            // getLastAppliedTermIndex().getIndex())
            // - (Optional) write small metadata file into snapshotDir so you can
            // debug/verify snapshots
            // - Return snapshotIndex
            return getLastAppliedTermIndex().getIndex();
        }
    }

    /**
     * Constructor: sets up local DB connections and Ratis wiring.
     *
     * Crash recovery happens automatically AFTER raftServer is started:
     * - Ratis reads its state from its storage dir
     * - Ratis restores latest snapshot (if any)
     * - Ratis replays committed entries by calling applyTransaction(...)
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

        // Ratis state machine instance
        this.stateMachine = new MyStateMachine();

        // Cassandra connection
        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostString()).build();
        this.session = cluster.connect(myID);

        // Optional local dir you can use for metadata/debug
        this.snapshotDir = new File(RATIS_STORAGE_DIR + File.separator + myID);

        // ============================
        // RATIS BOOTSTRAP (YOU IMPLEMENT HERE)
        // ============================
        // TODO:
        // 1) Build RaftGroup (groupId + list of peers)
        // 2) Build RaftProperties:
        // - set storage dir (RATIS_STORAGE_DIR/<myID>)
        // - configure snapshot/log retention policy for bounded logs
        // 3) Create + start raftServer:
        // - setStateMachine(stateMachine)
        // - raftServer.start()
        // 4) Create raftClient targeting raftGroup for proposals from
        // handleMessageFromClient
        //
        // NOTE: Once raftServer.start() runs, recovery is automatic:
        // - Ratis loads snapshots/logs and replays committed entries via
        // applyTransaction(...)
    }

    /**
     * CLIENT ENTRYPOINT:
     * Called by your networking layer when a client request arrives.
     *
     * This method should NOT apply commands to Cassandra directly.
     * It should only propose them to Raft (so Raft decides order).
     * The reply happens later in applyTransaction(...) after commit.
     */
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // ============================
        // CLIENT RECEIVE PATH (YOU IMPLEMENT HERE)
        // ============================
        // TODO:
        // 1) Create requestId (e.g., UUID string)
        // 2) pendingRequests.put(requestId, header.sndr)
        // 3) Build commandPayload bytes that include requestId + original client
        // request bytes
        // 4) raftClient.io().send(...) to propose payload into Raft log
        // 5) Do NOT reply here; reply after commit in applyTransaction(...)
    }

    /**
     * Shutdown:
     * You should stop Ratis components and Cassandra resources cleanly.
     */
    @Override
    public void close() {
        // TODO:
        // - stop/close raftClient
        // - stop/close raftServer
        // - session.close()
        // - cluster.close()
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
