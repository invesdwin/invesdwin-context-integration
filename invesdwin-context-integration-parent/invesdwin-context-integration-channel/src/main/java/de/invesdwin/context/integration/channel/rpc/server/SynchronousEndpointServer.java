package de.invesdwin.context.integration.channel.rpc.server;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.session.SynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
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
            .newCachedThreadPool(SynchronousEndpointServer.class.getSimpleName() + "_REQUEST");

    private final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor;
    private final SerdeLookupConfig serdeLookupConfig;
    private Duration requestWaitInterval = ISynchronousEndpointSession.DEFAULT_REQUEST_WAIT_INTERVAL;
    @GuardedBy("this")
    private final Int2ObjectMap<SynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<SynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();
    @GuardedBy("this")
    private Future<?> requestFuture;

    public SynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor) {
        this(serverAcceptor, SerdeLookupConfig.DEFAULT);
    }

    public SynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final SerdeLookupConfig serdeLookupConfig) {
        this.serverAcceptor = serverAcceptor;
        this.serdeLookupConfig = serdeLookupConfig;
    }

    public synchronized <T> void register(final Class<? super T> serviceInterface, final T serviceImplementation) {
        final SynchronousEndpointService service = SynchronousEndpointService.newInstance(serdeLookupConfig,
                serviceInterface, serviceImplementation);
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
            serviceId_service_sync.clear();
            serviceId_service_copy = new Int2ObjectOpenHashMap<>();
        }
    }

    public SerdeLookupConfig getSerdeLookupConfig() {
        return serdeLookupConfig;
    }

    public SynchronousEndpointService getService(final int serviceId) {
        return serviceId_service_copy.get(serviceId);
    }

    private final class IoRunnable implements Runnable {
        private final List<SynchronousEndpointServerSession> serverSessions = new ArrayList<>();
        private final LoopInterruptedCheck heartbeatLoopInterruptedCheck = new LoopInterruptedCheck(
                requestWaitInterval);
        private final ASpinWait throttle = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                //throttle while nothing to do, spin quickly while work is available
                boolean handled;
                do {
                    handled = false;
                    for (int i = 0; i < serverSessions.size(); i++) {
                        final SynchronousEndpointServerSession serverSession = serverSessions.get(i);
                        try {
                            handled |= serverSession.handle();
                        } catch (final EOFException e) {
                            //session closed
                            serverSessions.remove(i);
                            Closeables.closeQuietly(serverSession);
                            i--;
                        }
                    }
                    if (heartbeatLoopInterruptedCheck.check()) {
                        //force heartbeat check now
                        return false;
                    }
                } while (handled);
                return handled;
            }
        };

        @Override
        public void run() {
            try {
                while (accept()) {
                    if (!throttle.awaitFulfill(System.nanoTime(), requestWaitInterval)) {
                        //only check heartbeat interval when there is no more work or when the requestWaitInterval is reached
                        for (int i = 0; i < serverSessions.size(); i++) {
                            final SynchronousEndpointServerSession serverSession = serverSessions.get(i);
                            if (serverSession.isHeartbeatTimeout()) {
                                //session closed
                                Err.process(new TimeoutException(
                                        "Heartbeat timeout [" + serverSession.getEndpointSession().getHeartbeatTimeout()
                                                + "] exceeded: " + serverSession.getEndpointSession().getSessionId()));
                                serverSessions.remove(i);
                                Closeables.closeQuietly(serverSession);
                                i--;
                            }
                        }
                    }
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
                    //use latest request wait interval so that we don't need this in the constructor of the server
                    requestWaitInterval = endpointSession.getRequestWaitInterval();
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
