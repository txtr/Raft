package io.hamster.protocols.raft.proto.impl;

import io.grpc.stub.StreamObserver;
import io.hamster.protocols.raft.proto.RaftServerProtocol;
import io.hamster.protocols.raft.protocol.*;
import io.hamster.protocols.raft.transport.ServiceFactory;
import io.hamster.protocols.raft.transport.ServiceRegistry;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * gRPC server protocol.
 */
public class GrpcServerProtocol extends RaftServiceGrpc.RaftServiceImplBase implements RaftServerProtocol {

    private static final ConnectException CONNECT_EXCEPTION = new ConnectException();

    static {
        CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
    }

    private Function<PollRequest, CompletableFuture<PollResponse>> pollHandler;
    private Function<VoteRequest, CompletableFuture<VoteResponse>> voteHandler;
    private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;

    private final ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory;

    public GrpcServerProtocol(ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory,
                              ServiceRegistry registry) {
        this.factory = factory;
        registry.register(this);
    }

    @Override
    public void poll(PollRequest request, StreamObserver<PollResponse> responseObserver) {
        if(pollHandler != null){
            CompletableFuture<PollResponse> future = pollHandler.apply(request).whenComplete((r,t)->{
                if(t != null){
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(r);
                }
                responseObserver.onCompleted();
            });
        } else {
            responseObserver.onError(CONNECT_EXCEPTION);
        }

    }

    @Override
    public void vote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        super.vote(request, responseObserver);
    }

    @Override
    public void append(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
        super.append(request, responseObserver);
    }

    public <R> CompletableFuture<R> execute(String server, BiConsumer<RaftServiceGrpc.RaftServiceStub, StreamObserver<R>> callback) {
        CompletableFuture<R> future = new CompletableFuture();
        callback.accept(factory.getService(server), new StreamObserver<R>() {
            @Override
            public void onNext(R value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {

            }
        });
        return future;
    }

    @Override
    public CompletableFuture<PollResponse> poll(String server, PollRequest request) {
        return execute(server, (stub, observer) -> stub.poll(request, observer));
    }

    @Override
    public CompletableFuture<VoteResponse> vote(String server, VoteRequest request) {
        return execute(server, (stub, observer) -> stub.vote(request, observer));
    }

    @Override
    public CompletableFuture<AppendResponse> append(String server, AppendRequest request) {
        return execute(server, (stub, observer) -> stub.append(request, observer));
    }

    @Override
    public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
        this.pollHandler = handler;
    }

    @Override
    public void unregisterPollHandler() {
        this.pollHandler = null;
    }

    @Override
    public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
        this.voteHandler = handler;
    }

    @Override
    public void unregisterVoteHandler() {
        this.voteHandler = null;
    }

    @Override
    public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
        this.appendHandler = handler;
    }

    @Override
    public void unregisterAppendHandler() {
        this.appendHandler = null;
    }
}
