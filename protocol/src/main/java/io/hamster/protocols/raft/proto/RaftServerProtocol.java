package io.hamster.protocols.raft.proto;

import io.hamster.protocols.raft.protocol.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Raft Server Protocol
 */
public interface RaftServerProtocol {

    /**
     * Sends a poll request to the given node.
     *
     * @param server  the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<PollResponse> poll(String server, PollRequest request);

    /**
     * Sends a vote request to given node.
     *
     * @param server  the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with response
     */
    CompletableFuture<VoteResponse> vote(String server, VoteRequest request);

    /**
     * Sends an append request to the given node.
     *
     * @param server  the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<AppendResponse> append(String server, AppendRequest request);

    /**
     * Registers a poll request callback.
     *
     * @param handler the poll request handler to register
     */
    void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler);

    /**
     * Unregisters the poll request handler.
     */
    void unregisterPollHandler();

    /**
     * Registers a vote request callback.
     *
     * @param handler the vote request handler to register
     */
    void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler);

    /**
     * Unregisters the vote request handler.
     */
    void unregisterVoteHandler();

    /**
     * Registers an append request callback.
     *
     * @param handler the append request handler to register
     */
    void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler);

    /**
     * Unregisters the append request handler.
     */
    void unregisterAppendHandler();
}
