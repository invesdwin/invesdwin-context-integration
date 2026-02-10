package de.invesdwin.context.integration.channel.stream.server.async;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.AAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.stream.server.IStreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.StreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.context.integration.channel.stream.server.session.manager.DefaultStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableList;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.MaintenanceIntervalException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.closeable.Closeables;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class StreamAsynchronousEndpointServerHandlerFactory extends AAsynchronousEndpointServerHandlerFactory
        implements IStreamSynchronousEndpointServer {

    public static final int DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SESSION = StreamSynchronousEndpointServer.DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SESSION;
    public static final int DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SUBSCRIPTION = StreamSynchronousEndpointServer.DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SUBSCRIPTION;

    public static final int DEFAULT_MAX_IO_THREAD_COUNT = ASynchronousEndpointServer.DEFAULT_MAX_IO_THREAD_COUNT;
    public static final int DEFAULT_CREATE_IO_THREAD_SESSION_THRESHOLD = ASynchronousEndpointServer.DEFAULT_CREATE_IO_THREAD_SESSION_THRESHOLD;
    public static final WrappedExecutorService DEFAULT_IO_EXECUTOR = ASynchronousEndpointServer.DEFAULT_IO_EXECUTOR;
    public static final WrappedExecutorService DEFAULT_WORK_EXECUTOR = StreamSynchronousEndpointServer.DEFAULT_WORK_EXECUTOR;

    private static final IoRunnable[] IO_RUNNABLE_EMPTY_ARRAY = new IoRunnable[0];
    private static final int ROOT_IO_RUNNABLE_ID = 0;

    private final Int2ObjectMap<IStreamSynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<IStreamSynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();
    private final IStreamSynchronousEndpointServiceFactory serviceFactory;
    private final int maxSuccessivePushCountPerSession;
    private final int maxSuccessivePushCountPerSubscription;
    private final Map<String, StreamAsynchronousEndpointServerHandlerSession> sessionId_sessionManager = ILockCollectionFactory
            .getInstance(true)
            .newConcurrentMap();
    @GuardedBy("this")
    private IFastIterableList<IoRunnable> ioRunnables;
    private final int maxIoThreadCount;
    private final int createIoThreadSessionThreshold;
    private final WrappedExecutorService ioExecutor;

    public StreamAsynchronousEndpointServerHandlerFactory(
            final IStreamSynchronousEndpointServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
        this.maxIoThreadCount = newMaxIoThreadCount();
        if (maxIoThreadCount < 0) {
            throw new IllegalArgumentException(
                    "maxIoThreadCount should be greater than or equal to 0: " + maxIoThreadCount);
        }
        this.createIoThreadSessionThreshold = newCreateIoThreadSessionThreshold();
        if (createIoThreadSessionThreshold < 1) {
            throw new IllegalArgumentException("createIoThreadSessionThreshold should be greater than or equal to 1: "
                    + createIoThreadSessionThreshold);
        }
        this.ioExecutor = newIoExecutor();
        this.maxSuccessivePushCountPerSession = newMaxSuccessivePushCountPerSession();
        this.maxSuccessivePushCountPerSubscription = newMaxSuccessivePushCountPerSubscription();
    }

    /**
     * This defines how many consecutive topic messages can be pushed before checking for the next request and giving
     * that request priority in handling before pushing again
     */
    protected int newMaxSuccessivePushCountPerSession() {
        return DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SESSION;
    }

    /**
     * This defines how many consecutive topic messages can be pushed for an individual subscription before checking the
     * next subscription. This allows to give other topics the chance to send messages without being blocking by a
     * particularly busy topic.
     */
    protected int newMaxSuccessivePushCountPerSubscription() {
        return DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SUBSCRIPTION;
    }

    protected int newCreateIoThreadSessionThreshold() {
        return DEFAULT_CREATE_IO_THREAD_SESSION_THRESHOLD;
    }

    @Override
    public final int getMaxSuccessivePushCountPerSession() {
        return maxSuccessivePushCountPerSession;
    }

    @Override
    public final int getMaxSuccessivePushCountPerSubscription() {
        return maxSuccessivePushCountPerSubscription;
    }

    /**
     * Using multiple IO threads can be benefitial for throughput. More IO threads are added when more clients connect.
     * After heartbeat timeout and an IO threads being empty the IO thread is scaled down again.
     */
    protected int newMaxIoThreadCount() {
        return DEFAULT_MAX_IO_THREAD_COUNT;
    }

    /**
     * Should always be a CachedExecutorService (default) or another implementation that has a maximum size >=
     * newMaxIoThreadCount(). Otherwise IO threads will starve to death and requests will not be processed on those IO
     * runnables that are beyond the capacity of the IO executor.
     */
    protected WrappedExecutorService newIoExecutor() {
        return DEFAULT_IO_EXECUTOR;
    }

    public final WrappedExecutorService getIoExecutor() {
        return ioExecutor;
    }

    @Override
    protected WrappedExecutorService newWorkExecutor() {
        return DEFAULT_WORK_EXECUTOR;
    }

    @Override
    public final Duration getHeartbeatTimeout() {
        return super.getHeartbeatTimeout();
    }

    @Override
    public final Duration getRequestTimeout() {
        return super.getRequestTimeout();
    }

    @Override
    public synchronized void open() throws IOException {
        if (ioRunnables != null) {
            throw new IllegalStateException("already opened");
        }
        ioRunnables = ILockCollectionFactory.getInstance(false).newFastIterableArrayList(maxIoThreadCount);
    }

    @Override
    public synchronized void close() throws IOException {
        if (ioRunnables != null) {
            for (int i = 0; i < ioRunnables.size(); i++) {
                final IoRunnable ioRunnable = ioRunnables.get(i);
                ioRunnable.close();
            }
            ioRunnables = null;
        }
        for (final IStreamSynchronousEndpointService service : serviceId_service_sync.values()) {
            try {
                service.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
        }
        serviceId_service_sync.clear();
        serviceId_service_copy = null;
    }

    @Override
    public IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> newHandler() {
        return new StreamAsynchronousEndpointServerHandler(this);
    }

    @Override
    public IStreamSynchronousEndpointService getOrCreateService(final int serviceId, final String topic,
            final IProperties parameters) throws IOException {
        final IStreamSynchronousEndpointService service = getService(serviceId);
        if (service != null) {
            StreamServerMethodInfo.assertServiceTopic(service, topic);
            return service;
        } else {
            return registerService(serviceId, topic, parameters);
        }
    }

    @Override
    public IStreamSynchronousEndpointService getService(final int serviceId) {
        return serviceId_service_copy.get(serviceId);
    }

    private synchronized IStreamSynchronousEndpointService registerService(final int serviceId, final String topic,
            final IProperties parameters) throws IOException {
        final IStreamSynchronousEndpointService existing = serviceId_service_sync.get(serviceId);
        if (existing != null) {
            StreamServerMethodInfo.assertServiceTopic(existing, topic);
            return existing;
        }
        final IStreamSynchronousEndpointService service = serviceFactory.newService(serviceId, topic, parameters);
        service.open();
        Assertions.checkNull(serviceId_service_sync.put(service.getServiceId(), service));
        //create a new copy of the map so that server thread does not require synchronization
        this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
        return service;
    }

    public synchronized <T> boolean unregister(final int serviceId) {
        final IStreamSynchronousEndpointService removed = serviceId_service_sync.remove(serviceId);
        if (removed != null) {
            Closeables.closeQuietly(removed);
            //create a new copy of the map so that server thread does not require synchronization
            this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
        return new DefaultStreamSessionManager(session);
    }

    public StreamAsynchronousEndpointServerHandlerSession getOrCreateSession(
            final IAsynchronousHandlerContext<IByteBufferProvider> context) {
        final StreamAsynchronousEndpointServerHandlerSession session = sessionId_sessionManager
                .computeIfAbsent(context.getSessionId(), (k) -> {
                    return newSession(context);
                });
        return session;
    }

    private StreamAsynchronousEndpointServerHandlerSession newSession(
            final IAsynchronousHandlerContext<IByteBufferProvider> context) {
        final StreamAsynchronousEndpointServerHandlerSession session = new StreamAsynchronousEndpointServerHandlerSession(
                this, context);
        maybeIncreaseIoRunnableCount();
        assignServerSessionToIoRunnable(session);
        return session;
    }

    private void maybeIncreaseIoRunnableCount() {
        if (ioRunnables.isEmpty()) {
            //main runnable adds more threads on demand
            final IoRunnable rootIoRunnable = new IoRunnable(ROOT_IO_RUNNABLE_ID);
            final ListenableFuture<?> future = getIoExecutor().submit(rootIoRunnable);
            rootIoRunnable.setFuture(future);
            ioRunnables.add(rootIoRunnable);
        } else if (ioRunnables.size() < maxIoThreadCount) {
            int maxIoRunnableId = 0;
            final IoRunnable[] ioRunnablesArray = ioRunnables.asArray(IO_RUNNABLE_EMPTY_ARRAY);
            for (int i = 0; i < ioRunnablesArray.length; i++) {
                final IoRunnable ioRunnable = ioRunnablesArray[i];
                if (ioRunnable.serverSessions.size() < createIoThreadSessionThreshold) {
                    //no need to increase io runnables
                    return;
                }
                maxIoRunnableId = Integers.max(maxIoRunnableId, ioRunnable.ioRunnableId);
            }
            final IoRunnable newIoRunnable = new IoRunnable(maxIoRunnableId + 1);
            final ListenableFuture<?> future = getIoExecutor().submit(newIoRunnable);
            newIoRunnable.setFuture(future);
            ioRunnables.add(newIoRunnable);
        }
    }

    private void assignServerSessionToIoRunnable(final StreamAsynchronousEndpointServerHandlerSession session) {
        //assign session to the request runnable thread that hsa the least amount of sessions
        int minSessions = Integer.MAX_VALUE;
        int minSessionsIndex = -1;
        final IoRunnable[] ioRunnablesArray = ioRunnables.asArray(IO_RUNNABLE_EMPTY_ARRAY);
        for (int i = 0; i < ioRunnablesArray.length; i++) {
            final IoRunnable ioRunnable = ioRunnables.get(i);
            if (ioRunnable.getFuture() == null) {
                //already closed
                continue;
            }
            final int sessions = ioRunnable.serverSessions.size();
            if (sessions == 0) {
                minSessions = 0;
                minSessionsIndex = i;
                break;
            }
            if (sessions < minSessions) {
                minSessions = sessions;
                minSessionsIndex = i;
            }
        }
        final IoRunnable assignedIoRunnable = ioRunnablesArray[minSessionsIndex];
        assignedIoRunnable.serverSessions.add(session);

        final Closeable contextCloseable = new Closeable() {
            @Override
            public void close() throws IOException {
                session.close();
                Assertions.checkTrue(assignedIoRunnable.serverSessions.remove(session));
                final String sessionId = session.getContext().getSessionId();
                Assertions.checkSame(session, sessionId_sessionManager.remove(sessionId));
            }
        };
        session.getContext().registerCloseable(contextCloseable);
        final Closeable sessionCloseable = new Closeable() {
            @Override
            public void close() throws IOException {
                session.getContext().unregisterCloseable(contextCloseable);
            }
        };
        session.registerCloseable(sessionCloseable);
    }

    private final class IoRunnable implements Runnable, Closeable {
        private final IFastIterableList<StreamAsynchronousEndpointServerHandlerSession> serverSessions = ILockCollectionFactory
                .getInstance(true)
                .newFastIterableArrayList();
        private final LoopInterruptedCheck requestWaitLoopInterruptedCheck = new LoopInterruptedCheck(
                getRequestWaitInterval());
        private final LoopInterruptedCheck heartbeatLoopInterruptedCheck = new LoopInterruptedCheck(
                getHeartbeatInterval());
        private final ASpinWait throttle = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                //throttle while nothing to do, spin quickly while work is available
                boolean handledOverall = false;
                boolean handledNow;
                do {
                    handledNow = false;
                    final StreamAsynchronousEndpointServerHandlerSession[] serverSessionsArray = serverSessions
                            .asArray(StreamAsynchronousEndpointServerHandlerSession.EMPTY_ARRAY);
                    int removedServerSessions = 0;
                    for (int i = 0; i < serverSessionsArray.length; i++) {
                        final StreamAsynchronousEndpointServerHandlerSession serverSession = serverSessionsArray[i];
                        try {
                            handledNow |= serverSession.getManager().handle();
                            handledOverall |= handledNow;
                        } catch (final EOFException e) {
                            //session closed
                            serverSessions.remove(i - removedServerSessions);
                            Closeables.closeQuietly(serverSession);
                            removedServerSessions++;
                            final String sessionId = serverSession.getContext().getSessionId();
                            Assertions.checkSame(serverSession, sessionId_sessionManager.remove(sessionId));
                        }
                    }
                    if (requestWaitLoopInterruptedCheck.check()) {
                        if (handledOverall) {
                            //update server thread heartbeat timestamp
                            lastHeartbeatNanos = System.nanoTime();
                        }
                        //maybe check heartbeat and maybe accept more clients
                        throw MaintenanceIntervalException.getInstance("check heartbeat");
                    }
                } while (handledNow);
                return handledOverall;
            }
        };
        private final int ioRunnableId;
        private volatile Future<?> future;
        private final IFastIterableList<IoRunnable> ioRunnablesCopy;
        @GuardedBy("volatile because primary runnable checks the heartbeat of the other runnables")
        private volatile long lastHeartbeatNanos = System.nanoTime();

        private IoRunnable(final int ioRunnableId) {
            this.ioRunnableId = ioRunnableId;
            if (ioRunnableId == ROOT_IO_RUNNABLE_ID) {
                ioRunnablesCopy = ioRunnables;
            } else {
                ioRunnablesCopy = null;
            }
        }

        public void setFuture(final Future<?> future) {
            this.future = future;
        }

        public Future<?> getFuture() {
            return future;
        }

        @Override
        public synchronized void close() {
            if (future != null) {
                future.cancel(true);
                future = null;
            }
        }

        public boolean isEmptyAndHeartbeatTimeout() {
            return serverSessions.isEmpty()
                    && getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    process();
                }
            } catch (final Throwable t) {
                if (Throwables.isCausedByInterrupt(t)) {
                    //end
                    return;
                } else {
                    Err.process(new RuntimeException("ignoring", t));
                }
            } finally {
                final StreamAsynchronousEndpointServerHandlerSession[] serverSessionsArray = serverSessions
                        .asArray(StreamAsynchronousEndpointServerHandlerSession.EMPTY_ARRAY);
                for (int i = 0; i < serverSessionsArray.length; i++) {
                    Closeables.closeQuietly(serverSessionsArray[i]);
                }
                serverSessions.clear();
                if (ioRunnableId == ROOT_IO_RUNNABLE_ID) {
                    final IoRunnable[] ioRunnablesArray = ioRunnablesCopy.asArray(IO_RUNNABLE_EMPTY_ARRAY);
                    for (int i = 0; i < ioRunnablesArray.length; i++) {
                        ioRunnablesArray[i].close();
                    }
                }
                future = null;
            }
        }

        private void process() throws Exception {
            if (serverSessions.isEmpty()) {
                //reduce cpu cycles aggressively when no sessions are connected
                FTimeUnit.MILLISECONDS.sleep(1);
            } else {
                try {
                    if (!throttle.awaitFulfill(System.nanoTime(), getRequestWaitInterval())) {
                        maybeCheckHeartbeat();
                    }
                } catch (final MaintenanceIntervalException e) {
                    maybeCheckHeartbeat();
                }
            }
        }

        private void maybeCheckHeartbeat() throws InterruptedException, IOException {
            //only check heartbeat interval when there is no more work or when the requestWaitInterval is reached
            if (heartbeatLoopInterruptedCheck.check()) {
                checkServerSessionsHeartbeat();
                if (ioRunnableId == ROOT_IO_RUNNABLE_ID) {
                    checkIoRunnablesHeartbeat();
                }
            }
        }

        private void checkServerSessionsHeartbeat() {
            final StreamAsynchronousEndpointServerHandlerSession[] serverSessionsArray = serverSessions
                    .asArray(StreamAsynchronousEndpointServerHandlerSession.EMPTY_ARRAY);
            int removedSessions = 0;
            for (int i = 0; i < serverSessionsArray.length; i++) {
                final StreamAsynchronousEndpointServerHandlerSession serverSession = serverSessionsArray[i];
                if (serverSession.isHeartbeatTimeout()) {
                    Err.process(new TimeoutException("Heartbeat timeout [" + serverSession.getHeartbeatTimeout()
                            + "] exceeded: " + serverSession.getContext().getSessionId()));
                    serverSessions.remove(i - removedSessions);
                    removedSessions++;
                    Closeables.closeQuietly(serverSession);
                } else if (serverSession.isClosed()) {
                    serverSessions.remove(i - removedSessions);
                    removedSessions++;
                }
            }
        }

        private void checkIoRunnablesHeartbeat() throws IOException {
            final IoRunnable[] ioRunnablesArray = ioRunnablesCopy.asArray(IO_RUNNABLE_EMPTY_ARRAY);
            int removedIoRunnables = 0;
            //don't check heartbeat of root IO runnable (otherwise new clients cannot be accepted)
            for (int i = 1; i < ioRunnablesArray.length; i++) {
                final IoRunnable ioRunnable = ioRunnablesArray[i];
                if (ioRunnable.isEmptyAndHeartbeatTimeout()) {
                    ioRunnable.close();
                    ioRunnablesCopy.remove(i - removedIoRunnables);
                    removedIoRunnables++;
                }
            }
        }

    }

}
