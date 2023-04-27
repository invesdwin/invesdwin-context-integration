package de.invesdwin.context.integration.channel.rpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.duration.Duration;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Possible server types:
 * 
 * - each client a separate thread for IO and work
 * 
 * - all clients share one thread for IO and work
 * 
 * - one io thread, multiple worker threads, marshalling in IO
 * 
 * - one io thread, multiple worker threads, marshalling in worker
 * 
 * - multiple io threads (sharding?), multiple worker threads, marshalling in IO
 * 
 * - multiple io threads (sharding?), multiple worker threads, marshalling in worker
 * 
 */
@ThreadSafe
public class SynchronousEndpointServer implements ISynchronousChannel {

    private static final WrappedExecutorService REQUEST_EXECUTOR = Executors
            .newCachedThreadPool(SynchronousEndpointServer.class.getSimpleName() + "_REQUERST");
    private static final WrappedExecutorService RESPONSE_EXECUTOR = Executors.newFixedThreadPool(
            SynchronousEndpointServer.class.getSimpleName() + "_RESPONSE", Executors.getCpuThreadPoolCount());

    private final ISynchronousReader<ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider>> serverAcceptor;
    private final ISerde<Object[]> requestSerde;
    private final ISerde<Object> responseSerde;
    private final Duration requestTimeout;
    @GuardedBy("this")
    private final Int2ObjectMap<SynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<SynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();
    @GuardedBy("this")
    private Future<?> requestFuture;

    public SynchronousEndpointServer(
            final ISynchronousReader<ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider>> serverAcceptor,
            final ISerde<Object[]> requestSerde, final ISerde<Object> responseSerde, final Duration requestTimeout) {
        this.serverAcceptor = serverAcceptor;
        this.requestSerde = requestSerde;
        this.responseSerde = responseSerde;
        this.requestTimeout = requestTimeout;
    }

    public synchronized <T> void register(final Class<? super T> serviceInterface, final T serviceImplementation) {
        final SynchronousEndpointService service = SynchronousEndpointService.newInstance(serviceInterface,
                serviceImplementation);
        final SynchronousEndpointService existing = serviceId_service_sync.putIfAbsent(service.getServiceId(), service);
        if (existing != null) {
            throw new IllegalStateException("Already registered [" + service + "] as [" + existing + "]");
        }

        //create a new copy of the map so that server thread does not require synchronization
        this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
    }

    public synchronized <T> boolean unregister(final Class<? super T> serviceInterface) {
        final int serviceId = SynchronousEndpointService.newServiceId(serviceInterface);
        final SynchronousEndpointService removed = serviceId_service_sync.remove(serviceId);
        return removed != null;
    }

    @Override
    public synchronized void open() throws IOException {
        if (requestFuture != null) {
            throw new IllegalStateException("already opened");
        }
        serverAcceptor.open();
        requestFuture = REQUEST_EXECUTOR.submit(new IoRunnable());
    }

    @Override
    public synchronized void close() throws IOException {
        if (requestFuture != null) {
            requestFuture.cancel(true);
            requestFuture = null;
            serverAcceptor.close();
        }
    }

    private final class IoRunnable implements Runnable {
        private final List<Future<ICloseableByteBuffer>> workerFutures = new ArrayList<>();

        @Override
        public void run() {
            try {
                return;
                //                while (true) {
                //                    final boolean hasNext;
                //                    try {
                //                        hasNext = serverAcceptor.hasNext();
                //                    } catch (final EOFException e) {
                //                        //closed
                //                        return;
                //                    }
                //                    if (hasNext) {
                //                        final SocketSynchronousChannel channel;
                //                        try {
                //                            channel = serverAcceptor.readMessage();
                //                        } finally {
                //                            serverAcceptor.readFinished();
                //                        }
                //
                //                        final ISynchronousWriter<IByteBufferProvider> writer = FinancialdataHistoricalResolverRegistryClient
                //                                .newWriter(channel);
                //                        final ISynchronousReader<IByteBufferProvider> reader = FinancialdataHistoricalResolverRegistryClient
                //                                .newReader(channel);
                //                        try {
                //                            writer.open();
                //                            reader.open();
                //                            final SynchronousWriterSpinWait<IByteBufferProvider> writerSpinWait = SynchronousWriterSpinWaitPool
                //                                    .borrowObject(writer);
                //                            final SynchronousReaderSpinWait<IByteBufferProvider> readerSpinWait = SynchronousReaderSpinWaitPool
                //                                    .borrowObject(reader);
                //                            try (ICloseableByteBuffer buffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
                //                                final int requestSize = readerSpinWait
                //                                        .waitForRead(SynchronousChannelInfo.REQUEST_TIMEOUT)
                //                                        .getBuffer(buffer);
                //                                final String request = StringUtf8Serde.GET.fromBuffer(buffer.sliceTo(requestSize));
                //
                //                                final String[] requestSplit = Strings.splitPreserveAllTokens(request,
                //                                        SynchronousChannelInfo.SEPARATOR);
                //                                if (requestSplit.length != 2) {
                //                                    throw new IllegalArgumentException("Expected format [<register|unregister>"
                //                                            + SynchronousChannelInfo.SEPARATOR + "<pid>] but got: [" + request + "]");
                //                                }
                //                                final String command = requestSplit[0];
                //                                final String pid = requestSplit[1];
                //                                final String response;
                //                                if ("register".equals(command)) {
                //                                    response = register(pid).toString();
                //                                } else if ("unregister".equals(command)) {
                //                                    unregister(pid);
                //                                    response = FinancialdataHistoricalResolverRegistryClient.UNREGISTER_RESPONSE_OK;
                //                                } else {
                //                                    throw UnknownArgumentException.newInstance(String.class, command);
                //                                }
                //                                final int responseSize = StringUtf8Serde.GET.toBuffer(buffer, response);
                //                                writerSpinWait.waitForWrite(buffer.sliceTo(responseSize),
                //                                        SynchronousChannelInfo.REQUEST_TIMEOUT);
                //                            } finally {
                //                                SynchronousReaderSpinWaitPool.returnObject(readerSpinWait);
                //                                SynchronousWriterSpinWaitPool.returnObject(writerSpinWait);
                //                            }
                //                        } catch (final EOFException e) {
                //                            //closed on the other side
                //                        } catch (final IOException e) {
                //                            throw new RuntimeException(e);
                //                        } finally {
                //                            Closeables.closeQuietly(reader);
                //                            Closeables.closeQuietly(writer);
                //                        }
                //                    }
                //                    FTimeUnit.MILLISECONDS.sleep(100);
                //                }
            } catch (final Throwable t) {
                if (Throwables.isCausedByInterrupt(t)) {
                    //end
                    return;
                } else {
                    Err.process(new RuntimeException("ignoring", t));
                }
            } finally {
                cancelRemainingWorkerFutures();
                Closeables.closeQuietly(serverAcceptor);
            }

            //            try {
            //                while (true) {
            //                    //TODO accept new clients, look for requests in clients, dispatch request handling and response sending to worker (handle heartbeat as well), return client for request monitoring after completion
            //                    if (serverAcceptor.hasNext()) {
            //                        try {
            //                            final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint = serverAcceptor
            //                                    .readMessage();
            //                        } finally {
            //                            serverAcceptor.readFinished();
            //                        }
            //                    }
            //                    //reject executions if too many pending count for worker pool
            //                    //check on start of worker task if timeout is already exceeded and abort directly (might have been in queue for too long)
            //                    //maybe return exceptions to clients (similar to RmiExceptions that contain the stacktrace as message, full stacktrace in testing only?)
            //                    //handle writeFinished in io thread (maybe the better idea?)
            //                    return;
            //                }
            //            } finally {
            //                cancelRemainingWorkerFutures();
            //            }
        }

        private void cancelRemainingWorkerFutures() {
            if (!workerFutures.isEmpty()) {
                for (int i = 0; i < workerFutures.size(); i++) {
                    workerFutures.get(i).cancel(true);
                }
                workerFutures.clear();
            }
        }
    }

}
