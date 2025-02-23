package de.invesdwin.context.integration.channel.stream;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
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
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientChannel;
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientReader;
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientWriter;
import de.invesdwin.context.integration.channel.stream.server.StreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.async.StreamAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.stream.server.sessionless.StreamSessionlessSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class StreamThroughputChannelTest extends ThroughputChannelTest {

    public static final int STREAM_CLIENT_TRANSPORTS = StreamLatencyChannelTest.STREAM_CLIENT_TRANSPORTS;
    public static final boolean STREAM_CLIENT_LAZY = StreamLatencyChannelTest.STREAM_CLIENT_LAZY;
    public static final int STREAM_TEST_THREADS = StreamLatencyChannelTest.STREAM_TEST_THREADS;
    public static final IStreamSynchronousEndpointServiceFactory STREAM_SERVICE_FACTORY = StreamLatencyChannelTest.STREAM_SERVICE_FACTORY;

    public StreamThroughputChannelTest(final AChannelTest parent) {
        super(parent);
    }

    public void runStreamThroughputTest(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamThroughputTestLazy(serverAcceptor, clientEndpointFactory);
        } else {
            runStreamThroughputTestEager(serverAcceptor, clientEndpointFactory);
        }
    }

    private void runStreamThroughputTestLazy(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
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
            runStreamThroughputTest(() -> serverClient, () -> clientClient);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(clientClient);
            Closeables.closeQuietly(serverClient);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    private void runStreamThroughputTestEager(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
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
            runStreamThroughputTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    public void runStreamHandlerThroughputTest(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamHandlerThroughputTestLazy(serverFactory, clientEndpointFactory);
        } else {
            runStreamHandlerThroughputTestEager(serverFactory, clientEndpointFactory);
        }
    }

    private void runStreamHandlerThroughputTestLazy(
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
            runStreamThroughputTest(() -> serverClient, () -> clientClient);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(clientClient);
            Closeables.closeQuietly(serverClient);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    private void runStreamHandlerThroughputTestEager(
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
            runStreamThroughputTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    public void runStreamBlockingThroughputTest(
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
            runStreamThroughputTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    public void runStreamSessionlessThroughputTest(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamSessionlessThroughputTestLazy(serverEndpointFactory, clientEndpointFactory);
        } else {
            runStreamSessionlessThroughputTestEager(serverEndpointFactory, clientEndpointFactory);
        }
    }

    private void runStreamSessionlessThroughputTestLazy(
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
            runStreamThroughputTest(() -> serverClient, () -> clientClient);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(clientClient);
            Closeables.closeQuietly(serverClient);
            Closeables.closeQuietly(serverChannel);
        }
    }

    @SuppressWarnings({ "resource" })
    private void runStreamSessionlessThroughputTestEager(
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
            runStreamThroughputTest(clientFactory, clientFactory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
        }
    }

    public void runStreamThroughputTest(final Supplier<IStreamSynchronousEndpointClient> serverClientFactory,
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
                        runStreamThroughputTest(serverClient, clientClient, String.valueOf(index));
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
        final String name = "runStreamThroughputTest_parallelTests";
        if (threads <= 1) {
            return Executors.newDisabledExecutor(name);
        } else {
            return Executors.newFixedThreadPool(name, threads);
        }
    }

    public void runStreamThroughputTest(final IStreamSynchronousEndpointClient serverClient,
            final IStreamSynchronousEndpointClient clientClient, final String topicSuffix) {
        final String channelTopic = "channel" + topicSuffix;
        final StreamSynchronousEndpointClientChannel senderChannel = newStreamSynchronousEndpointClientChannel(
                clientClient, channelTopic, null);
        final ISynchronousWriter<FDate> senderChannelWriter = AChannelTest
                .newSerdeWriter(newStreamSynchronousEndpointClientWriter(senderChannel));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(senderChannelWriter);
        final StreamSynchronousEndpointClientChannel receiverChannel = newStreamSynchronousEndpointClientChannel(
                serverClient, channelTopic, null);
        final ISynchronousReader<FDate> receiverChannelReader = AChannelTest
                .newSerdeReader(newStreamSynchronousEndpointClientReader(receiverChannel));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(parent, receiverChannelReader);
        try {
            runThroughputTest(senderTask, receiverTask);
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
        return StreamLatencyChannelTest.maybeDebug(manager);
    }

    public static IStreamSynchronousEndpointSession maybeDebug(final IStreamSynchronousEndpointSession session) {
        return StreamLatencyChannelTest.maybeDebug(session);
    }

    public static IStreamSynchronousEndpointClient maybeDebug(final IStreamSynchronousEndpointClient client) {
        return StreamLatencyChannelTest.maybeDebug(client);
    }

    public static IStreamSynchronousEndpointServiceFactory maybeDebug(
            final IStreamSynchronousEndpointServiceFactory serviceFactory) {
        return StreamLatencyChannelTest.maybeDebug(serviceFactory);
    }

}
