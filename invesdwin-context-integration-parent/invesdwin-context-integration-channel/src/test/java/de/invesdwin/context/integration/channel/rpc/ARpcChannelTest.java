package de.invesdwin.context.integration.channel.rpc;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient;
import de.invesdwin.context.integration.channel.rpc.client.session.multi.MultipleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.client.session.multi.SingleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.DefaultSynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.service.IRpcTestService;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcClientTask;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestService;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.server.sessionless.SessionlessSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public abstract class ARpcChannelTest extends AChannelTest {

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
        serverChannel.register(IRpcTestService.class, new RpcTestService());
        final SynchronousEndpointClient<IRpcTestService> client = new SynchronousEndpointClient<>(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                }, IRpcTestService.class);
        try {
            final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTest_client",
                    newRpcClientThreads());
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
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
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
        serverChannel.register(IRpcTestService.class, new RpcTestService());
        final SynchronousEndpointClient<IRpcTestService>[] clients = new SynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new SynchronousEndpointClient<>(
                    new SingleMultiplexingSynchronousEndpointClientSessionPool(
                            new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)),
                    IRpcTestService.class);
        }
        try {
            final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTest_client",
                    newRpcClientThreads());
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
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
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
        handlerFactory.register(IRpcTestService.class, new RpcTestService());
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final SynchronousEndpointClient<IRpcTestService> client = new SynchronousEndpointClient<>(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                }, IRpcTestService.class);
        try {
            final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTest_client",
                    newRpcClientThreads());
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
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings("unchecked")
    private void runRpcHandlerPerformanceTestEager(
            final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        handlerFactory.register(IRpcTestService.class, new RpcTestService());
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final SynchronousEndpointClient<IRpcTestService>[] clients = new SynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new SynchronousEndpointClient<>(
                    new SingleMultiplexingSynchronousEndpointClientSessionPool(
                            new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)),
                    IRpcTestService.class);
        }
        try {
            final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTest_client",
                    newRpcClientThreads());
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
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
        }
    }

    protected void runRpcPerformanceTest(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        if (RPC_CLIENT_LAZY) {
            runRpcPerformanceTestLazy(serverEndpointFactory, clientEndpointFactory, mode);
        } else {
            runRpcPerformanceTestEager(serverEndpointFactory, clientEndpointFactory, mode);
        }
    }

    private void runRpcPerformanceTestLazy(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        handlerFactory.register(IRpcTestService.class, new RpcTestService());
        final SessionlessSynchronousEndpointServer serverChannel = new SessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final SynchronousEndpointClient<IRpcTestService> client = new SynchronousEndpointClient<>(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                }, IRpcTestService.class);
        try {
            final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTest_client",
                    newRpcClientThreads());
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
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings("unchecked")
    private void runRpcPerformanceTestEager(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final AsynchronousEndpointServerHandlerFactory handlerFactory = new AsynchronousEndpointServerHandlerFactory();
        handlerFactory.register(IRpcTestService.class, new RpcTestService());
        final SessionlessSynchronousEndpointServer serverChannel = new SessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final SynchronousEndpointClient<IRpcTestService>[] clients = new SynchronousEndpointClient[RPC_CLIENT_TRANSPORTS];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new SynchronousEndpointClient<>(
                    new SingleMultiplexingSynchronousEndpointClientSessionPool(
                            new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)),
                    IRpcTestService.class);
        }
        try {
            final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTest_client",
                    newRpcClientThreads());
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
            for (int i = 0; i < clients.length; i++) {
                Closeables.closeQuietly(clients[i]);
            }
            Closeables.closeQuietly(serverChannel);
        }
    }

}
