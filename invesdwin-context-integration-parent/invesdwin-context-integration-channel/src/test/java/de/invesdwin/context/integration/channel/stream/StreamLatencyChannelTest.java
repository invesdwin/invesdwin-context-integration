package de.invesdwin.context.integration.channel.stream;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.MultipleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.SingleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.single.SingleplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.DefaultSynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.stream.client.BlockingStreamSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.stream.client.IStreamSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.stream.client.LoggingDelegateStreamSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientChannel;
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientReader;
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientWriter;
import de.invesdwin.context.integration.channel.stream.server.StreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.async.StreamAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.stream.server.service.log.LoggingDelegateStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.stream.server.session.manager.log.LoggingDelegateStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.log.LoggingDelegateStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.stream.server.sessionless.StreamSessionlessSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.service.PipeStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.closeable.Closeables;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class StreamLatencyChannelTest extends LatencyChannelTest {

    public static final int STREAM_CLIENT_TRANSPORTS = AChannelTest.DEBUG ? 1 : 2;
    public static final boolean STREAM_CLIENT_LAZY = true;
    public static final int STREAM_TEST_THREADS = 1;
    public static final IStreamSynchronousEndpointServiceFactory STREAM_SERVICE_FACTORY = PipeStreamSynchronousEndpointServiceFactory.INSTANCE;
    public static final boolean VERBOSE_DEBUG = false;

    public StreamLatencyChannelTest(final AChannelTest parent) {
        super(parent);
    }

    public void runStreamLatencyTest(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamLatencyTestLazy(serverAcceptor, clientEndpointFactory);
        } else {
            runStreamLatencyTestEager(serverAcceptor, clientEndpointFactory);
        }
    }

    private void runStreamLatencyTestLazy(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamSynchronousEndpointServer serverChannel = new StreamSynchronousEndpointServer(serverAcceptor,
                newStreamServiceFactory()) {
            @Override
            protected int newMaxIoThreadCount() {
                return STREAM_CLIENT_TRANSPORTS;
            }

            @Override
            public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
                return maybeDebug(super.newManager(maybeDebug(session)));
            }
        };
        final IStreamSynchronousEndpointClient serverClient = newStreamSynchronousEndpointClient(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return STREAM_CLIENT_TRANSPORTS;
                    }
                });
        final IStreamSynchronousEndpointClient clientClient = newStreamSynchronousEndpointClient(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return STREAM_CLIENT_TRANSPORTS;
                    }
                });
        try {
            serverChannel.open();
            runStreamLatencyTest(() -> serverClient, () -> clientClient);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(clientClient);
            Closeables.closeQuietly(serverClient);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    private void runStreamLatencyTestEager(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamSynchronousEndpointServer serverChannel = new StreamSynchronousEndpointServer(serverAcceptor,
                newStreamServiceFactory()) {
            @Override
            protected int newMaxIoThreadCount() {
                return STREAM_CLIENT_TRANSPORTS;
            }

            @Override
            public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
                return maybeDebug(super.newManager(maybeDebug(session)));
            }
        };
        final Supplier<IStreamSynchronousEndpointClient> clientFactory = () -> newStreamSynchronousEndpointClient(
                new SingleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)));
        try {
            serverChannel.open();
            runStreamLatencyTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    public void runStreamHandlerLatencyTest(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamHandlerLatencyTestLazy(serverFactory, clientEndpointFactory);
        } else {
            runStreamHandlerLatencyTestEager(serverFactory, clientEndpointFactory);
        }
    }

    private void runStreamHandlerLatencyTestLazy(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                newStreamServiceFactory()) {
            @Override
            public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
                return maybeDebug(super.newManager(maybeDebug(session)));
            }
        };
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final IStreamSynchronousEndpointClient serverClient = newStreamSynchronousEndpointClient(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return STREAM_CLIENT_TRANSPORTS;
                    }
                });
        final IStreamSynchronousEndpointClient clientClient = newStreamSynchronousEndpointClient(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return STREAM_CLIENT_TRANSPORTS;
                    }
                });
        try {
            serverChannel.open();
            runStreamLatencyTest(() -> serverClient, () -> clientClient);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(clientClient);
            Closeables.closeQuietly(serverClient);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    private void runStreamHandlerLatencyTestEager(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                newStreamServiceFactory()) {
            @Override
            public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
                return maybeDebug(super.newManager(maybeDebug(session)));
            }
        };
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final Supplier<IStreamSynchronousEndpointClient> clientFactory = () -> newStreamSynchronousEndpointClient(
                new SingleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)));
        try {
            serverChannel.open();
            runStreamLatencyTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    public void runStreamBlockingLatencyTest(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                newStreamServiceFactory()) {
            @Override
            public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
                return maybeDebug(super.newManager(maybeDebug(session)));
            }
        };
        final IAsynchronousChannel serverChannel = serverFactory.apply(handlerFactory);
        final Supplier<IStreamSynchronousEndpointClient> clientFactory = () -> newStreamSynchronousEndpointClient(
                new SingleplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)));
        try {
            serverChannel.open();
            runStreamLatencyTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    public void runStreamSessionlessLatencyTest(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamSessionlessLatencyTestLazy(serverEndpointFactory, clientEndpointFactory);
        } else {
            runStreamSessionlessLatencyTestEager(serverEndpointFactory, clientEndpointFactory);
        }
    }

    private void runStreamSessionlessLatencyTestLazy(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                newStreamServiceFactory()) {
            @Override
            public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
                return maybeDebug(super.newManager(maybeDebug(session)));
            }
        };
        final StreamSessionlessSynchronousEndpointServer serverChannel = new StreamSessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final IStreamSynchronousEndpointClient serverClient = newStreamSynchronousEndpointClient(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return STREAM_CLIENT_TRANSPORTS;
                    }
                });
        final IStreamSynchronousEndpointClient clientClient = newStreamSynchronousEndpointClient(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return STREAM_CLIENT_TRANSPORTS;
                    }
                });
        try {
            serverChannel.open();
            runStreamLatencyTest(() -> serverClient, () -> clientClient);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(clientClient);
            Closeables.closeQuietly(serverClient);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    private void runStreamSessionlessLatencyTestEager(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                newStreamServiceFactory()) {
            @Override
            public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
                return maybeDebug(super.newManager(maybeDebug(session)));
            }
        };
        final StreamSessionlessSynchronousEndpointServer serverChannel = new StreamSessionlessSynchronousEndpointServer(
                serverEndpointFactory, handlerFactory);
        final Supplier<IStreamSynchronousEndpointClient> clientFactory = () -> newStreamSynchronousEndpointClient(
                new SingleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)));
        try {
            serverChannel.open();
            runStreamLatencyTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    public void runStreamLatencyTest(final Supplier<IStreamSynchronousEndpointClient> serverClientFactory,
            final Supplier<IStreamSynchronousEndpointClient> clientClientFactory) throws InterruptedException {
        final WrappedExecutorService testExecutor = newTestExecutor();
        final IStreamSynchronousEndpointClient[] serverClients = new IStreamSynchronousEndpointClient[STREAM_CLIENT_TRANSPORTS];
        final IStreamSynchronousEndpointClient[] clientClients = new IStreamSynchronousEndpointClient[STREAM_CLIENT_TRANSPORTS];
        for (int i = 0; i < serverClients.length; i++) {
            serverClients[i] = serverClientFactory.get();
            clientClients[i] = clientClientFactory.get();
        }
        try {
            int curClient = 0;
            try (IBufferingIterator<Future<?>> testFutures = new BufferingIterator<>()) {
                for (int i = 0; i < newStreamTestThreads(); i++) {
                    final int index = i;
                    final IStreamSynchronousEndpointClient serverClient = serverClients[curClient];
                    final IStreamSynchronousEndpointClient clientClient = clientClients[curClient];
                    testFutures.add(testExecutor.submit(() -> {
                        runStreamLatencyTest(serverClient, clientClient, String.valueOf(index));
                    }));
                    curClient++;
                    if (curClient >= serverClients.length) {
                        curClient = 0;
                    }
                }
                while (testFutures.hasNext()) {
                    Futures.getNoInterrupt(testFutures.next());
                }
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            testExecutor.shutdownNow();
            testExecutor.awaitTermination();
            for (int i = 0; i < serverClients.length; i++) {
                Closeables.closeQuietly(serverClients[i]);
                Closeables.closeQuietly(clientClients[i]);
            }
        }
    }

    protected WrappedExecutorService newTestExecutor() {
        final int threads = newStreamTestThreads();
        final String name = "runStreamLatencyTest_parallelTests";
        if (threads <= 1) {
            return Executors.newDisabledExecutor(name);
        } else {
            return Executors.newFixedThreadPool(name, threads);
        }
    }

    public void runStreamLatencyTest(final IStreamSynchronousEndpointClient serverClient,
            final IStreamSynchronousEndpointClient clientClient, final String topicSuffix) {
        final String requestTopic = "request" + topicSuffix;
        final String responseTopic = "response" + topicSuffix;
        final StreamSynchronousEndpointClientChannel serverRequestChannel = newStreamSynchronousEndpointClientChannel(
                serverClient, requestTopic, null);
        final StreamSynchronousEndpointClientChannel serverResponseChannel = newStreamSynchronousEndpointClientChannel(
                serverClient, responseTopic, null);
        final ISynchronousReader<FDate> serverRequestReader = parent
                .newSerdeReader(newStreamSynchronousEndpointClientReader(serverRequestChannel));
        final ISynchronousWriter<FDate> serverResponseWriter = parent
                .newSerdeWriter(newStreamSynchronousEndpointClientWriter(serverResponseChannel));
        final LatencyServerTask serverTask = new LatencyServerTask(parent, serverRequestReader, serverResponseWriter);
        final StreamSynchronousEndpointClientChannel clientRequestChannel = newStreamSynchronousEndpointClientChannel(
                clientClient, requestTopic, null);
        final StreamSynchronousEndpointClientChannel clientResponseChannel = newStreamSynchronousEndpointClientChannel(
                clientClient, responseTopic, null);
        final ISynchronousWriter<FDate> clientRequestWriter = parent
                .newSerdeWriter(newStreamSynchronousEndpointClientWriter(clientRequestChannel));
        final ISynchronousReader<FDate> clientResponseReader = parent
                .newSerdeReader(newStreamSynchronousEndpointClientReader(clientResponseChannel));
        final LatencyClientTask clientTask = new LatencyClientTask(parent, clientRequestWriter, clientResponseReader);
        try {
            runLatencyTest(serverTask, clientTask);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public ISynchronousWriter<IByteBufferProvider> newStreamSynchronousEndpointClientWriter(
            final StreamSynchronousEndpointClientChannel channel) {
        return new StreamSynchronousEndpointClientWriter(channel);
    }

    public ISynchronousReader<IByteBufferProvider> newStreamSynchronousEndpointClientReader(
            final StreamSynchronousEndpointClientChannel channel) {
        return new StreamSynchronousEndpointClientReader(channel);
    }

    public StreamSynchronousEndpointClientChannel newStreamSynchronousEndpointClientChannel(
            final IStreamSynchronousEndpointClient client, final String topic, final Integer valueFixedLength) {
        return new StreamSynchronousEndpointClientChannel(client, topic, valueFixedLength);
    }

    public IStreamSynchronousEndpointClient newStreamSynchronousEndpointClient(
            final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool) {
        final IStreamSynchronousEndpointClient client = new BlockingStreamSynchronousEndpointClient(sessionPool);
        //        client = new AsyncDelegateSynchronousEndpointClient(client);
        return maybeDebug(client);
    }

    protected int newStreamTestThreads() {
        return STREAM_TEST_THREADS;
    }

    protected IStreamSynchronousEndpointServiceFactory newStreamServiceFactory() {
        return maybeDebug(STREAM_SERVICE_FACTORY);
    }

    public static IStreamSessionManager maybeDebug(final IStreamSessionManager manager) {
        if (VERBOSE_DEBUG) {
            return new LoggingDelegateStreamSessionManager(manager);
        } else {
            return manager;
        }
    }

    public static IStreamSynchronousEndpointSession maybeDebug(final IStreamSynchronousEndpointSession session) {
        if (VERBOSE_DEBUG) {
            return new LoggingDelegateStreamSynchronousEndpointSession(session);
        } else {
            return session;
        }
    }

    public static IStreamSynchronousEndpointClient maybeDebug(final IStreamSynchronousEndpointClient client) {
        if (VERBOSE_DEBUG) {
            return new LoggingDelegateStreamSynchronousEndpointClient(client);
        } else {
            return client;
        }
    }

    public static IStreamSynchronousEndpointServiceFactory maybeDebug(
            final IStreamSynchronousEndpointServiceFactory factory) {
        if (VERBOSE_DEBUG) {
            return new LoggingDelegateStreamSynchronousEndpointServiceFactory(factory);
        } else {
            return factory;
        }
    }

}
