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
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
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
                while (true) {
                    //TODO accept new clients, look for requests in clients, dispatch request handling and response sending to worker, return client for request monitoring after completion
                    return;
                }
            } finally {
                cancelRemainingWorkerFutures();
            }
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
