package io.hamster.protocols.raft.transport;

import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ServiceProviderImpl implements ServiceProvider {

    private final ClusterService clusterService;

    public ServiceProviderImpl(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public <T> ServiceFactory<T> getFactory(Function<Channel, T> factory) {
        return new ServiceFactoryImpl<>(factory);
    }

    private class ServiceFactoryImpl<T> implements ServiceFactory<T> {

        private final Function<Channel, T> factory;
        private final Map<Node, T> services = new ConcurrentHashMap<>();

        public ServiceFactoryImpl(Function<Channel, T> factory) {
            this.factory = factory;
        }

        private T getService(Node node) {
            T service = services.get(node);
            if (service == null) {
                service = services.compute(node, (id, value) -> {
                    if (value == null) {
                        value = factory.apply(getChannel(node.host(), node.port()));
                    }
                    return value;
                });
            }
            return service;
        }

        private Channel getChannel(String host, int port) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    /*.intercept(new ClientInterceptor() {
                        @Override
                        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                                @Override
                                public void start(Listener<RespT> responseListener, Metadata headers) {
                                    super.start(responseListener, headers);
                                }
                            };
                        }
                    })*/
                    .build();
            watchConnectivityState(channel);
            return channel;
        }

        private void watchConnectivityState(ManagedChannel channel) {
            ConnectivityState currentState = channel.getState(false);
            if (currentState != ConnectivityState.SHUTDOWN) {
                channel.notifyWhenStateChanged(currentState, () -> {
                    System.out.println("Previous-> " + currentState + ", Current State-> " + channel.getState(false));
                    watchConnectivityState(channel);
                });
            }
        }


        @Override
        public T getService(String nodeId) {
            return getService(clusterService.getNode(nodeId));
        }

        @Override
        public T getService(String host, int port) {
            return getService(host + ":" + port);
        }
    }
}
