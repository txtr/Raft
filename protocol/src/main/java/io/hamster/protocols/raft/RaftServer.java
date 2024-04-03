package io.hamster.protocols.raft;

import io.hamster.protocols.raft.proto.RaftServerProtocol;
import io.hamster.protocols.raft.storage.RaftStorage;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

public interface RaftServer {

    /**
     * Bootstraps the cluster using the provided cluster configuration.
     * <p>
     * Bootstrapping the cluster results in a new cluster being formed with the provided configuration. The initial
     * nodes in a cluster must always be bootstrapped. This is necessary to prevent split brain. If the provided
     * configuration is empty, the local server will form a single-node cluster.
     * <p>
     * Only {@link RaftMember.Type#ACTIVE} members can be included in a bootstrap configuration. If the local server is
     * not initialized as an active member, it cannot be part of the bootstrap configuration for the cluster.
     * <p>
     * When the cluster is bootstrapped, the local server will be transitioned into the active state and begin
     * participating in the Raft consensus algorithm. When the cluster is first bootstrapped, no leader will exist.
     * The bootstrapped members will elect a leader amongst themselves. Once a cluster has been bootstrapped, additional
     * members may be {@link #join(String...) joined} to the cluster. In the event that the bootstrapped members cannot
     * reach a quorum to elect a leader, bootstrap will continue until successful.
     * <p>
     * It is critical that all servers in a bootstrap configuration be started with the same exact set of members.
     * Bootstrapping multiple servers with different configurations may result in split brain.
     * <p>
     * The {@link CompletableFuture} returned by this method will be completed once the cluster has been bootstrapped,
     * a leader has been elected, and the leader has been notified of the local server's client configurations.
     *
     * @param cluster The bootstrap cluster configuration.
     * @return A completable future to be completed once the cluster has been bootstrapped.
     */
    CompletableFuture<RaftServer> bootstrap(Collection<String> cluster);

    /**
     * Builds a single-use Raft server.
     * <p>
     * This builder should be used to programmatically configure and construct a new {@link RaftServer} instance.
     * The builder provides methods for configuring all aspects of a Raft server. The {@code RaftServer.Builder}
     * class cannot be instantiated directly. To create a new builder, use one of the
     * {@link RaftServer#builder(String) server builder factory} methods.
     * <pre>
     *   {@code
     *   RaftServer.Builder builder = RaftServer.builder(address);
     *   }
     * </pre>
     * Once the server has been configured, use the {@link #build()} method to build the server instance:
     * <pre>
     *   {@code
     *   RaftServer server = RaftServer.builder(address)
     *     ...
     *     .build();
     *   }
     * </pre>
     * Each server <em>must</em> be configured with a {@link StateMachine}. The state machine is the component of the
     * server that stores state and reacts to commands and queries submitted by clients to the cluster. State machines
     * are provided to the server in the form of a state machine {@link Supplier factory} to allow the server to reconstruct
     * its state when necessary.
     * <pre>
     *   {@code
     *   RaftServer server = RaftServer.builder(address)
     *     .withStateMachine(MyStateMachine::new)
     *     .build();
     *   }
     * </pre>
     */
    abstract class Builder implements io.hamster.utils.Builder<RaftServer> {
        private static final Duration DEFAULT_ELECTION_TIMEOUT = Duration.ofMillis(750);
        private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMillis(250);
        private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(30);

        protected String name;
        protected String localServerId;
        protected RaftServerProtocol protocol;
        protected RaftStorage storage;

        protected Builder(String localServerId) {
            this.localServerId = checkNotNull(localServerId, "localServerId cannot be null");
        }

        /**
         * Sets the server name.
         * <p>
         * The server name is used to
         *
         * @param name The server name.
         * @return The server builder.
         */
        public Builder withName(String name) {
            this.name = checkNotNull(name, "name cannot be null");
            return this;
        }

        /**
         * Sets the server protocol.
         *
         * @param protocol The server protocol.
         * @return The server builder.
         */
        public Builder withProtocol(RaftServerProtocol protocol) {
            this.protocol = checkNotNull(protocol, "protocol cannot be null");
            return this;
        }

        /**
         * Sets the storage module.
         *
         * @param storage The storage module.
         * @return The Raft server builder.
         * @throws NullPointerException if {@code storage} is null
         */
        public Builder withStorage(RaftStorage storage) {
            this.storage = checkNotNull(storage, "storage cannot be null");
            return this;
        }
    }
}
