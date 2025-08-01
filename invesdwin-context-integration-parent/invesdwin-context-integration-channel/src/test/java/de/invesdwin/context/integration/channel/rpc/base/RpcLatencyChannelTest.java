package de.invesdwin.context.integration.channel.rpc.base;

import java.util.concurrent.Future;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.client.DefaultRpcSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.rpc.base.client.IRpcSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.MultipleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.SingleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.single.SingleplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.DefaultSynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.RpcSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.RpcAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.IRpcTestService;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcClientTask;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestService;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.sessionless.RpcSessionlessSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class RpcLatencyChannelTest extends LatencyChannelTest {

    public static final int RPC_CLIENT_THREADS = AChannelTest.DEBUG ? 1 : 10;
    public static final int RPC_CLIENT_TRANSPORTS = AChannelTest.DEBUG ? 1 : 2;
    public static final boolean RPC_CLIENT_LAZY = true;

    public RpcLatencyChannelTest(final AChannelTest parent) {
        super(parent);
    }

    public void runRpcPerformanceTest(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        if (RPC_CLIENT_LAZY) {
            runRpcPerformanceTestLazy(serverAcceptor, clientEndpointFactory, mode);
        } else {
            runRpcPerformanceTestEager(serverAcceptor, clientEndpointFactory, mode);
        }
    }

    private void runRpcPerformanceTestLazy(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final RpcSynchronousEndpointServer serverChannel = new RpcSynchronousEndpointServer(serverAcceptor) {
            @Override
            protected int newMaxIoThreadCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        final RpcTestService service = new RpcTestService(parent, newRpcClientThreads());
        serverChannel.register(IRpcTestService.class, service);
        final IRpcSynchronousEndpointClient<IRpcTestService> client = new DefaultRpcSynchronousEndpointClient<>(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                }, IRpcTestService.class);
        final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTestLazy_client",
                newRpcClientThreads());
        try {
            serverChannel.open();
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newRpcClientThreads(); i++) {
                    clientFutures
                            .add(clientExecutor.submit(new RpcClientTask(parent, client, String.valueOf(i + 1), mode)));
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            clientExecutor.shutdownNow();
            clientExecutor.awaitTermination();
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    @SuppressWarnings("unchecked")
    private void runRpcPerformanceTestEager(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final RpcSynchronousEndpointServer serverChannel = new RpcSynchronousEndpointServer(serverAcceptor) {
            @Override
            protected int newMaxIoThreadCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        final RpcTestService service = new RpcTestService(parent, newRpcClientThreads());
        serverChannel.register(IRpcTestService.class, service);
        final IRpcSynchronousEndpointClient<IRpcTestService>[] clients = new IRpcSynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new DefaultRpcSynchronousEndpointClient<>(
                    new SingleMultiplexingSynchronousEndpointClientSessionPool(
                            new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)),
                    IRpcTestService.class);
        }
        final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTestEager_client",
                newRpcClientThreads());
        try {
            serverChannel.open();
            int curClient = 0;
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newRpcClientThreads(); i++) {
                    clientFutures.add(clientExecutor
                            .submit(new RpcClientTask(parent, clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            clientExecutor.shutdownNow();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    public int newRpcClientThreads() {
        return RPC_CLIENT_THREADS;
    }

    public void runRpcHandlerPerformanceTest(
            final Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        if (RPC_CLIENT_LAZY) {
            runRpcHandlerPerformanceTestLazy(serverFactory, clientEndpointFactory, mode);
        } else {
            runRpcHandlerPerformanceTestEager(serverFactory, clientEndpointFactory, mode);
        }
    }

    private void runRpcHandlerPerformanceTestLazy(
            final Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final RpcAsynchronousEndpointServerHandlerFactory handlerFactory = new RpcAsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(parent, newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final IRpcSynchronousEndpointClient<IRpcTestService> client = new DefaultRpcSynchronousEndpointClient<>(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                }, IRpcTestService.class);
        final WrappedExecutorService clientExecutor = Executors
                .newFixedThreadPool("runRpcHandlerPerformanceTestLazy_client", newRpcClientThreads());
        try {
            serverChannel.open();
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newRpcClientThreads(); i++) {
                    clientFutures
                            .add(clientExecutor.submit(new RpcClientTask(parent, client, String.valueOf(i + 1), mode)));
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            clientExecutor.shutdownNow();
            clientExecutor.awaitTermination();
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    @SuppressWarnings("unchecked")
    private void runRpcHandlerPerformanceTestEager(
            final Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final RpcAsynchronousEndpointServerHandlerFactory handlerFactory = new RpcAsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(parent, newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final IRpcSynchronousEndpointClient<IRpcTestService>[] clients = new IRpcSynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new DefaultRpcSynchronousEndpointClient<>(
                    new SingleMultiplexingSynchronousEndpointClientSessionPool(
                            new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)),
                    IRpcTestService.class);
        }
        final WrappedExecutorService clientExecutor = Executors
                .newFixedThreadPool("runRpcHandlerPerformanceTestEager_client", newRpcClientThreads());
        try {
            serverChannel.open();
            int curClient = 0;
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newRpcClientThreads(); i++) {
                    clientFutures.add(clientExecutor
                            .submit(new RpcClientTask(parent, clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            clientExecutor.shutdownNow();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    @SuppressWarnings("unchecked")
    public void runRpcBlockingPerformanceTest(
            final Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final RpcAsynchronousEndpointServerHandlerFactory handlerFactory = new RpcAsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(parent, newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final IRpcSynchronousEndpointClient<IRpcTestService>[] clients = new IRpcSynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new DefaultRpcSynchronousEndpointClient<>(
                    new SingleplexingSynchronousEndpointClientSessionPool(
                            new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)),
                    IRpcTestService.class);
        }
        final WrappedExecutorService clientExecutor = Executors
                .newFixedThreadPool("runRpcBlockingPerformanceTest_client", newRpcClientThreads());
        try {
            serverChannel.open();
            int curClient = 0;
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newRpcClientThreads(); i++) {
                    clientFutures.add(clientExecutor
                            .submit(new RpcClientTask(parent, clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            clientExecutor.shutdownNow();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    public void runRpcSessionlessPerformanceTest(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        if (RPC_CLIENT_LAZY) {
            runRpcSessionlessPerformanceTestLazy(serverEndpointFactory, clientEndpointFactory, mode);
        } else {
            runRpcSessionlessPerformanceTestEager(serverEndpointFactory, clientEndpointFactory, mode);
        }
    }

    private void runRpcSessionlessPerformanceTestLazy(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final RpcAsynchronousEndpointServerHandlerFactory handlerFactory = new RpcAsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(parent, newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final RpcSessionlessSynchronousEndpointServer serverChannel = new RpcSessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final IRpcSynchronousEndpointClient<IRpcTestService> client = new DefaultRpcSynchronousEndpointClient<>(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                }, IRpcTestService.class);
        final WrappedExecutorService clientExecutor = Executors
                .newFixedThreadPool("runRpcSessionlessPerformanceTestLazy_client", newRpcClientThreads());
        try {
            serverChannel.open();
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newRpcClientThreads(); i++) {
                    clientFutures
                            .add(clientExecutor.submit(new RpcClientTask(parent, client, String.valueOf(i + 1), mode)));
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            clientExecutor.shutdownNow();
            clientExecutor.awaitTermination();
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    @SuppressWarnings("unchecked")
    private void runRpcSessionlessPerformanceTestEager(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final RpcAsynchronousEndpointServerHandlerFactory handlerFactory = new RpcAsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(parent, newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final RpcSessionlessSynchronousEndpointServer serverChannel = new RpcSessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final IRpcSynchronousEndpointClient<IRpcTestService>[] clients = new IRpcSynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new DefaultRpcSynchronousEndpointClient<>(
                    new SingleMultiplexingSynchronousEndpointClientSessionPool(
                            new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)),
                    IRpcTestService.class);
        }
        final WrappedExecutorService clientExecutor = Executors
                .newFixedThreadPool("runRpcSessionlessPerformanceTestEager_client", newRpcClientThreads());
        try {
            serverChannel.open();
            int curClient = 0;
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newRpcClientThreads(); i++) {
                    clientFutures.add(clientExecutor
                            .submit(new RpcClientTask(parent, clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            clientExecutor.shutdownNow();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

}
