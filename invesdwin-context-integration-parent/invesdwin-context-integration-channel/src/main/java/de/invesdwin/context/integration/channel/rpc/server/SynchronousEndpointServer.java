package de.invesdwin.context.integration.channel.rpc.server;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.session.SynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.rpc.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.marshallers.serde.ISerde;
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

    private final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor;
    private final ISerde<Object[]> requestSerde;
    private final ISerde<Object> responseSerde;
    private final Duration requestTimeout;
    @GuardedBy("this")
    private final Int2ObjectMap<SynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<SynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();
    @GuardedBy("this")
    private Future<?> requestFuture;

    public SynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
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

    public SynchronousEndpointService getService(final int serviceId) {
        return serviceId_service_copy.get(serviceId);
    }

    private final class IoRunnable implements Runnable {
        private final List<SynchronousEndpointServerSession> serverSessions = new ArrayList<>();
        private final ASpinWait throttle = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                //throttle while nothing to do, spin quickly while work is available
                boolean handled;
                do {
                    handled = false;
                    for (int i = 0; i < serverSessions.size(); i++) {
                        final SynchronousEndpointServerSession serverSession = serverSessions.get(i);
                        handled |= serverSession.handle();
                    }
                } while (handled);
                return handled;
            }
        };

        @Override
        public void run() {
            try {
                while (accept()) {
                    throttle.awaitFulfill(System.nanoTime());
                }
            } catch (final Throwable t) {
                if (Throwables.isCausedByInterrupt(t)) {
                    //end
                    return;
                } else {
                    Err.process(new RuntimeException("ignoring", t));
                }
            } finally {
                if (!serverSessions.isEmpty()) {
                    for (int i = 0; i < serverSessions.size(); i++) {
                        Closeables.closeQuietly(serverSessions.get(i));
                    }
                    serverSessions.clear();
                }
                Closeables.closeQuietly(serverAcceptor);
            }

            //            try {
            //                while (true) {
            //                    //TODO look for requests in clients, dispatch request handling and response sending to worker (handle heartbeat as well), return client for request monitoring after completion
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

        private boolean accept() throws IOException {
            //accept new clients
            final boolean hasNext;
            try {
                hasNext = serverAcceptor.hasNext();
            } catch (final EOFException e) {
                //server closed
                return false;
            }
            if (hasNext) {
                try {
                    final ISynchronousEndpointSession endpointSession = serverAcceptor.readMessage();
                    serverSessions
                            .add(new SynchronousEndpointServerSession(SynchronousEndpointServer.this, endpointSession));
                } finally {
                    serverAcceptor.readFinished();
                }
            }
            return true;
        }
    }

}
