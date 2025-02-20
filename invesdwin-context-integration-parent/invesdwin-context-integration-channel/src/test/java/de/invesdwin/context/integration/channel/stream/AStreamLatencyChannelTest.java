package de.invesdwin.context.integration.channel.stream;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
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
import de.invesdwin.context.integration.channel.stream.server.sessionless.StreamSessionlessSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.timeseriesdb.service.TimeSeriesDBStreamSynchronousEndpointServiceFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class AStreamLatencyChannelTest extends ALatencyChannelTest {

    public static final int STREAM_CLIENT_TRANSPORTS = DEBUG ? 1 : 2;
    public static final boolean STREAM_CLIENT_LAZY = true;
    private static final int STREAM_TEST_THREADS = 1;

    protected int newStreamTestThreads() {
        return STREAM_TEST_THREADS;
    }

    protected IStreamSynchronousEndpointServiceFactory newServiceFactory() {
        return TimeSeriesDBStreamSynchronousEndpointServiceFactory.INSTANCE;
    }

    protected void runStreamPerformanceTest(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamPerformanceTestLazy(serverAcceptor, serviceFactory, clientEndpointFactory);
        } else {
            runStreamPerformanceTestEager(serverAcceptor, serviceFactory, clientEndpointFactory);
        }
    }

    private void runStreamPerformanceTestLazy(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamSynchronousEndpointServer serverChannel = new StreamSynchronousEndpointServer(serverAcceptor,
                serviceFactory) {
            @Override
            protected int newMaxIoThreadCount() {
                return STREAM_CLIENT_TRANSPORTS;
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
    private void runStreamPerformanceTestEager(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamSynchronousEndpointServer serverChannel = new StreamSynchronousEndpointServer(serverAcceptor,
                serviceFactory) {
            @Override
            protected int newMaxIoThreadCount() {
                return STREAM_CLIENT_TRANSPORTS;
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

    protected void runStreamHandlerPerformanceTest(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamHandlerPerformanceTestLazy(serverFactory, serviceFactory, clientEndpointFactory);
        } else {
            runStreamHandlerPerformanceTestEager(serverFactory, serviceFactory, clientEndpointFactory);
        }
    }

    private void runStreamHandlerPerformanceTestLazy(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                serviceFactory);
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
    private void runStreamHandlerPerformanceTestEager(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                serviceFactory);
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
    protected void runStreamBlockingPerformanceTest(
            final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                serviceFactory);
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

    protected void runStreamSessionlessPerformanceTest(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        if (STREAM_CLIENT_LAZY) {
            runStreamSessionlessPerformanceTestLazy(serverEndpointFactory, serviceFactory, clientEndpointFactory);
        } else {
            runStreamSessionlessPerformanceTestEager(serverEndpointFactory, serviceFactory, clientEndpointFactory);
        }
    }

    private void runStreamSessionlessPerformanceTestLazy(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                serviceFactory);
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
    private void runStreamSessionlessPerformanceTestEager(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final IStreamSynchronousEndpointServiceFactory serviceFactory,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory)
            throws InterruptedException {
        final StreamAsynchronousEndpointServerHandlerFactory handlerFactory = new StreamAsynchronousEndpointServerHandlerFactory(
                serviceFactory);
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

    protected void runStreamLatencyTest(final Supplier<IStreamSynchronousEndpointClient> serverClientFactory,
            final Supplier<IStreamSynchronousEndpointClient> clientClientFactory) throws InterruptedException {
        final WrappedExecutorService testExecutor = Executors.newFixedThreadPool("runStreamLatencyTest_parallelTests",
                newStreamTestThreads());
        final IStreamSynchronousEndpointClient[] serverClients = new IStreamSynchronousEndpointClient[STREAM_CLIENT_TRANSPORTS];
        final IStreamSynchronousEndpointClient[] clientClients = new IStreamSynchronousEndpointClient[STREAM_CLIENT_TRANSPORTS];
        for (int i = 0; i < serverClients.length; i++) {
            serverClients[i] = serverClientFactory.get();
            clientClients[i] = clientClientFactory.get();
        }
        try {
            int curClient = 0;
            for (int i = 0; i < newStreamTestThreads(); i++) {
                final int index = i;
                final IStreamSynchronousEndpointClient serverClient = serverClients[curClient];
                final IStreamSynchronousEndpointClient clientClient = clientClients[curClient];
                testExecutor.execute(() -> {
                    runStreamLatencyTest(serverClient, clientClient, String.valueOf(index));
                });
                curClient++;
                if (curClient >= serverClients.length) {
                    curClient = 0;
                }
            }
        } finally {
            testExecutor.shutdown();
            testExecutor.awaitTermination();
            for (int i = 0; i < serverClients.length; i++) {
                Closeables.closeQuietly(serverClients[i]);
                Closeables.closeQuietly(clientClients[i]);
            }
        }
    }

    protected void runStreamLatencyTest(final IStreamSynchronousEndpointClient serverClient,
            final IStreamSynchronousEndpointClient clientClient, final String topicSuffix) {
        final StreamSynchronousEndpointClientChannel serverRequestChannel = newStreamSynchronousEndpointClientChannel(
                serverClient, "request" + topicSuffix, getMaxMessageSize());
        final StreamSynchronousEndpointClientChannel serverResponseChannel = newStreamSynchronousEndpointClientChannel(
                serverClient, "response" + topicSuffix, getMaxMessageSize());
        final ISynchronousReader<FDate> serverRequestReader = newSerdeReader(
                newStreamSynchronousEndpointClientReader(serverRequestChannel));
        final ISynchronousWriter<FDate> serverResponseWriter = newSerdeWriter(
                newStreamSynchronousEndpointClientWriter(serverResponseChannel));
        final LatencyServerTask serverTask = new LatencyServerTask(serverRequestReader, serverResponseWriter);
        final StreamSynchronousEndpointClientChannel clientRequestChannel = newStreamSynchronousEndpointClientChannel(
                clientClient, "request" + topicSuffix, getMaxMessageSize());
        final StreamSynchronousEndpointClientChannel clientResponseChannel = newStreamSynchronousEndpointClientChannel(
                clientClient, "response" + topicSuffix, getMaxMessageSize());
        final ISynchronousWriter<FDate> clientRequestWriter = newSerdeWriter(
                newStreamSynchronousEndpointClientWriter(clientRequestChannel));
        final ISynchronousReader<FDate> clientResponseReader = newSerdeReader(
                newStreamSynchronousEndpointClientReader(clientResponseChannel));
        final LatencyClientTask clientTask = new LatencyClientTask(clientRequestWriter, clientResponseReader);
        try {
            runLatencyTest(serverTask, clientTask);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected ISynchronousWriter<IByteBufferProvider> newStreamSynchronousEndpointClientWriter(
            final StreamSynchronousEndpointClientChannel channel) {
        return new StreamSynchronousEndpointClientWriter(channel);
    }

    protected ISynchronousReader<IByteBufferProvider> newStreamSynchronousEndpointClientReader(
            final StreamSynchronousEndpointClientChannel channel) {
        return new StreamSynchronousEndpointClientReader(channel);
    }

    protected StreamSynchronousEndpointClientChannel newStreamSynchronousEndpointClientChannel(
            final IStreamSynchronousEndpointClient client, final String topic, final Integer valueFixedLength) {
        return new StreamSynchronousEndpointClientChannel(client, topic, valueFixedLength);
    }

    protected IStreamSynchronousEndpointClient newStreamSynchronousEndpointClient(
            final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool) {
        final BlockingStreamSynchronousEndpointClient blocking = new BlockingStreamSynchronousEndpointClient(
                sessionPool);
        //        return new AsyncDelegateSynchronousEndpointClient(blocking);
        return blocking;
    }

}
