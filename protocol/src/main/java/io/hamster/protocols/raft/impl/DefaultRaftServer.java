package io.hamster.protocols.raft.impl;

import io.hamster.protocols.raft.RaftServer;
import io.hamster.protocols.raft.storage.RaftStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides a standalone implementation of the <a href="http://raft.github.io/">Raft consensus algorithm</a>.
 * @see RaftStorage
 */
public class DefaultRaftServer implements RaftServer {

    private final Logger log = LoggerFactory.getLogger(DefaultRaftServer.class);
    protected final RaftContext context;
    private volatile CompletableFuture<RaftServer> openFuture;
    private volatile boolean started;


    public DefaultRaftServer(RaftContext context){
        this.context = checkNotNull(context, "context cannot be null");
    }

    @Override
    public CompletableFuture<RaftServer> bootstrap(Collection<String> cluster) {
        return null;
    }
    /**
     * Default Raft server builder.
     */
    public class Builder extends RaftServer.Builder {

        protected Builder(String localServerId) {
            super(localServerId);
        }

        @Override
        public RaftServer build() {

            // If the server name is null, set it to the member ID.
            if (name == null) {
                name = localServerId;
            }

            // If the storage is not configured, create a new Storage instance with the configured serializer.
            if(storage == null){
                storage = RaftStorage.builder().build();
            }

            RaftContext raft = new RaftContext(
                    name,
                    localServerId,
                    protocol,
                    storage,
                    true);
            //raft.setElectionTimeout(electionTimeout);
            //raft.setHeartbeatInterval(heartbeatInterval);

            return new DefaultRaftServer(raft);
        }
    }
}
