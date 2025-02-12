package de.invesdwin.context.integration.channel.rpc.base;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.client.ISynchronousEndpointClient;
import de.invesdwin.context.integration.channel.rpc.base.client.SynchronousEndpointClient;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.MultipleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.SingleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.single.SingleplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.DefaultSynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.IRpcTestService;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcClientTask;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestService;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.sessionless.SessionlessSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public abstract class ARpcChannelTest extends ALatencyChannelTest {

    public static final int RPC_CLIENT_THREADS = DEBUG ? 1 : 10;
    public static final int RPC_CLIENT_TRANSPORTS = DEBUG ? 1 : 2;
    public static final boolean RPC_CLIENT_LAZY = true;

    protected void runRpcPerformanceTest(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
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
        final SynchronousEndpointServer serverChannel = new SynchronousEndpointServer(serverAcceptor) {
            @Override
            protected int newMaxIoThreadCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        final RpcTestService service = new RpcTestService(newRpcClientThreads());
        serverChannel.register(IRpcTestService.class, service);
        final ISynchronousEndpointClient<IRpcTestService> client = new SynchronousEndpointClient<>(
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
                    clientFutures.add(clientExecutor.submit(new RpcClientTask(client, String.valueOf(i + 1), mode)));
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            clientExecutor.shutdown();
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
        final SynchronousEndpointServer serverChannel = new SynchronousEndpointServer(serverAcceptor) {
            @Override
            protected int newMaxIoThreadCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        final RpcTestService service = new RpcTestService(newRpcClientThreads());
        serverChannel.register(IRpcTestService.class, service);
        final ISynchronousEndpointClient<IRpcTestService>[] clients = new ISynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new SynchronousEndpointClient<>(
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
                    clientFutures.add(
                            clientExecutor.submit(new RpcClientTask(clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            clientExecutor.shutdown();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    protected int newRpcClientThreads() {
        return RPC_CLIENT_THREADS;
    }

    protected void runRpcHandlerPerformanceTest(
            final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        if (RPC_CLIENT_LAZY) {
            runRpcHandlerPerformanceTestLazy(serverFactory, clientEndpointFactory, mode);
        } else {
            runRpcHandlerPerformanceTestEager(serverFactory, clientEndpointFactory, mode);
        }
    }

    private void runRpcHandlerPerformanceTestLazy(
            final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final ISynchronousEndpointClient<IRpcTestService> client = new SynchronousEndpointClient<>(
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
                    clientFutures.add(clientExecutor.submit(new RpcClientTask(client, String.valueOf(i + 1), mode)));
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            clientExecutor.shutdown();
            clientExecutor.awaitTermination();
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    @SuppressWarnings("unchecked")
    private void runRpcHandlerPerformanceTestEager(
            final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final ISynchronousEndpointClient<IRpcTestService>[] clients = new ISynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new SynchronousEndpointClient<>(
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
                    clientFutures.add(
                            clientExecutor.submit(new RpcClientTask(clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            clientExecutor.shutdown();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    @SuppressWarnings("unchecked")
    protected void runRpcBlockingPerformanceTest(
            final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final ISynchronousEndpointClient<IRpcTestService>[] clients = new ISynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new SynchronousEndpointClient<>(
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
                    clientFutures.add(
                            clientExecutor.submit(new RpcClientTask(clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            clientExecutor.shutdown();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

    protected void runRpcSessionlessPerformanceTest(
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
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final SessionlessSynchronousEndpointServer serverChannel = new SessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final ISynchronousEndpointClient<IRpcTestService> client = new SynchronousEndpointClient<>(
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
                    clientFutures.add(clientExecutor.submit(new RpcClientTask(client, String.valueOf(i + 1), mode)));
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            clientExecutor.shutdown();
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
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        final RpcTestService service = new RpcTestService(newRpcClientThreads());
        handlerFactory.register(IRpcTestService.class, service);
        final SessionlessSynchronousEndpointServer serverChannel = new SessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final ISynchronousEndpointClient<IRpcTestService>[] clients = new ISynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new SynchronousEndpointClient<>(
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
                    clientFutures.add(
                            clientExecutor.submit(new RpcClientTask(clients[curClient], String.valueOf(i + 1), mode)));
                    curClient++;
                    if (curClient >= clients.length) {
                        curClient = 0;
                    }
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            clientExecutor.shutdown();
            clientExecutor.awaitTermination();
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(service);
        }
    }

}
