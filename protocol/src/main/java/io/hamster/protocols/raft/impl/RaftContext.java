package io.hamster.protocols.raft.impl;

import io.hamster.protocols.raft.proto.RaftServerProtocol;
import io.hamster.protocols.raft.protocol.RaftServiceGrpc;
import io.hamster.protocols.raft.storage.RaftStorage;
import io.hamster.storage.StorageException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages the volatile state and state transitions of a Raft server.
 * <p>
 * This class is the primary vehicle for managing the state of a server. All state that is shared across roles (i.e. follower, candidate, leader)
 * is stored in the cluster state. This includes Raft-specific state like the current leader and term, the log, and the cluster configuration.
 */
public class RaftContext extends RaftServiceGrpc.RaftServiceImplBase implements AutoCloseable {

    protected final String name;
    protected final RaftServerProtocol protocol;
    protected final RaftStorage storage;

    public RaftContext(
            String name,
            String localServerId,
            RaftServerProtocol protocol,
            RaftStorage storage,
            boolean closeOnStop) {
        this.name = checkNotNull(name, "name cannot be null");
        this.protocol = checkNotNull(protocol, "protocol cannot be null");
        this.storage = checkNotNull(storage, "storage cannot be null");

        // Lock the storage directory.
        if (!storage.lock(localServerId)) {
            throw new StorageException("Failed to acquire storage lock; ensure each Raft server is configured with a distinct storage directory");
        }

    }

    @Override
    public void close() throws Exception {

    }
}
