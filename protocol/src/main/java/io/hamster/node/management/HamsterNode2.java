package io.hamster.node.management;

import io.hamster.protocols.raft.proto.impl.GrpcServerProtocol;
import io.hamster.protocols.raft.protocol.PollResponse;
import io.hamster.protocols.raft.protocol.RaftServiceGrpc;
import io.hamster.protocols.raft.transport.*;

import java.util.concurrent.CompletableFuture;

public class HamsterNode2 {

    public static void main(String args[]) throws Exception{

        ClusterService clusterService = new ClusterServiceImpl("2");
        clusterService.addNode(new Node("1","localhost",2008));
        clusterService.addNode(new Node("2","localhost",2009));
        clusterService.addNode(new Node("3","localhost",2010));

        System.out.println(clusterService.getNodes());

        ServiceRegistry serviceRegistry = new ServiceRegistryImpl(clusterService);
        ServiceProvider serviceProvider = new ServiceProviderImpl(clusterService);
        serviceRegistry.start();
        ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory = serviceProvider.getFactory(RaftServiceGrpc::newStub);
        GrpcServerProtocol grpcServerProtocol = new GrpcServerProtocol(factory,serviceRegistry);
        grpcServerProtocol.registerPollHandler(pollRequest -> {
            return CompletableFuture.completedFuture(PollResponse.newBuilder().setAccepted(true).build());
        });
        while (true){
            Thread.sleep(1000L);
        }
    }

}
