package io.hamster.node.management;

import io.hamster.protocols.raft.proto.impl.GrpcServerProtocol;
import io.hamster.protocols.raft.protocol.PollRequest;
import io.hamster.protocols.raft.protocol.PollResponse;
import io.hamster.protocols.raft.protocol.RaftServiceGrpc;
import io.hamster.protocols.raft.transport.*;

import java.util.concurrent.CompletableFuture;

public class HamsterNode {

    public static void main(String args[]) throws Exception{

        ClusterService clusterService = new ClusterServiceImpl("1");
        clusterService.addNode(new Node("1","localhost",2008));
        clusterService.addNode(new Node("2","localhost",2009));
        clusterService.addNode(new Node("3","localhost",2010));

        System.out.println(clusterService.getNodes());

        ServiceRegistry serviceRegistry = new ServiceRegistryImpl(clusterService);
        ServiceProvider serviceProvider = new ServiceProviderImpl(clusterService);
        serviceRegistry.start();
        ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory = serviceProvider.getFactory(RaftServiceGrpc::newStub);
        GrpcServerProtocol grpcServerProtocol = new GrpcServerProtocol(factory,serviceRegistry);
        while (true){
            PollRequest pollRequest = PollRequest.newBuilder()
                    .build();
            Thread.sleep(20L);
            CompletableFuture<PollResponse> future = grpcServerProtocol.poll("2",pollRequest);
            future.whenComplete((p,t)->{
               if(t != null){
                   System.out.println(t);
               } else {
                   System.out.println(p);
               }
            });
        }
    }

}
