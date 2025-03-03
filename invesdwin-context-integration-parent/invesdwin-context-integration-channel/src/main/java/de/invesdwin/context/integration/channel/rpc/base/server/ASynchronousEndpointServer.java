package de.invesdwin.context.integration.channel.rpc.base.server;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.session.ISynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableList;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.MaintenanceIntervalException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

/**
 * Possible server types:
 * 
 * - each client a separate thread for IO and work (unlimited is not a good idea, thus limited by maxIoThreadCount and
 * use null worker executor)
 * 
 * - all clients share one thread for IO and work (use maxIoThreadCount=1 and null worker executor)
 * 
 * - one io thread, multiple worker threads, marshalling in IO (not implemented, marshalling is always done by worker)
 * 
 * - one io thread, multiple worker threads, marshalling in worker (use maxIoThreadCount=1 and a fixed worker executor)
 * 
 * - multiple io threads (sharding?), multiple worker threads, marshalling in IO (not implemented, marshalling is always
 * done by worker)
 * 
 * - multiple io threads (sharding?), multiple worker threads, marshalling in worker (IO threads limited by
 * maxIoThreadCount and use a fixed worker executor)
 * 
 * This is a weak server implementation that does not use a selector or native polling mechanism. Instead each channel
 * is checked individually for requests. This is useful for channels where selector or a native polling mechanism is not
 * available (e.g. memory mapped files). It can also be used when latency is not so important (though it can cause
 * excessive amounts of slow syscalls). For all other cases it might be better to use a netty async handler or disni
 * (active) handler for the server.
 */
@ThreadSafe
public abstract class ASynchronousEndpointServer implements ISynchronousEndpointServer {

    public static final int DEFAULT_MAX_IO_THREAD_COUNT = 4;
    public static final int DEFAULT_CREATE_IO_THREAD_SESSION_THRESHOLD = 3;
    public static final WrappedExecutorService DEFAULT_IO_EXECUTOR = Executors
            .newCachedThreadPool(ASynchronousEndpointServer.class.getSimpleName() + "_IO")
            .setDynamicThreadName(false);
    public static final WrappedExecutorService DEFAULT_WORK_EXECUTOR = Executors
            .newFixedThreadPool(ASynchronousEndpointServer.class.getSimpleName() + "_WORK",
                    Executors.getCpuThreadPoolCount())
            .setDynamicThreadName(false);
    public static final int DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL = 10_000;
    public static final int DEFAULT_INITIAL_MAX_PENDING_WORK_COUNT_PER_SESSION = -50;

    private static final IoRunnable[] IO_RUNNABLE_EMPTY_ARRAY = new IoRunnable[0];
    private static final int ROOT_IO_RUNNABLE_ID = 0;

    private final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor;
    private Duration requestWaitInterval = ISynchronousEndpointSession.DEFAULT_REQUEST_WAIT_INTERVAL;
    private Duration heartbeatTimeout = ISynchronousEndpointSession.DEFAULT_HEARTBEAT_TIMEOUT;
    private Duration requestTimeout = ISynchronousEndpointSession.DEFAULT_REQUEST_TIMEOUT;
    private final Duration heartbeatInterval = ISynchronousEndpointSession.DEFAULT_HEARTBEAT_INTERVAL;
    @GuardedBy("this")
    private IFastIterableList<IoRunnable> ioRunnables;
    private final int maxIoThreadCount;
    private final int createIoThreadSessionThreshold;
    private final WrappedExecutorService ioExecutor;
    private final WrappedExecutorService workExecutor;
    private final int maxPendingWorkCountOverall;
    private final int initialMaxPendingWorkCountPerSession;
    private int maxPendingWorkCountPerSession;
    private final AtomicInteger activeSessionsOverall = new AtomicInteger();

    public ASynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor) {
        this.serverAcceptor = serverAcceptor;
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
        this.workExecutor = newWorkExecutor();
        this.maxPendingWorkCountOverall = newMaxPendingWorkCountOverall();
        if (maxPendingWorkCountOverall < 0) {
            throw new IllegalArgumentException(
                    "maxPendingWorkCountOverall should not be negative: " + maxPendingWorkCountOverall);
        }
        this.initialMaxPendingWorkCountPerSession = newInitialMaxPendingWorkCountPerSession();
        updateMaxPendingCountPerSession(0);
    }

    protected int newCreateIoThreadSessionThreshold() {
        return DEFAULT_CREATE_IO_THREAD_SESSION_THRESHOLD;
    }

    private void updateMaxPendingCountPerSession(final int activeSessions) {
        if (initialMaxPendingWorkCountPerSession == 0) {
            maxPendingWorkCountPerSession = 0;
        } else if (initialMaxPendingWorkCountPerSession > 0) {
            maxPendingWorkCountPerSession = initialMaxPendingWorkCountPerSession;
        } else {
            maxPendingWorkCountPerSession = Integers.max(Integers.divide(maxPendingWorkCountOverall, activeSessions),
                    -initialMaxPendingWorkCountPerSession);
        }
    }

    /**
     * Further requests will be rejected if the workExecutor has more than that amount of requests pending. Only applies
     * when workExecutor is not null.
     * 
     * return 0 here for unlimited pending work count overall.
     */
    protected int newMaxPendingWorkCountOverall() {
        return DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL;
    }

    /**
     * Return 0 here for unlimited pending work count per session, will be limited by overall pending work count only
     * which means that one rogue client can take all resources for himself (not advisable unless clients can be
     * trusted).
     * 
     * Return a positive value here to limit the pending requests
     */
    protected int newInitialMaxPendingWorkCountPerSession() {
        return DEFAULT_INITIAL_MAX_PENDING_WORK_COUNT_PER_SESSION;
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

    /**
     * Can return null here to not use an executor for work handling, instead the IO thread will handle it directly.
     * This is preferred when request execution is very fast does not involve complex calculations.
     * 
     * Use LIMITED_WORK_EXECUTOR when cpu intensive or long running tasks are performed that should not block the IO of
     * the request thread. In Java 21 it might be a good choice to use a virtual thread executor here so that IO and
     * sleeps don't block work on other threads (maybe use a higher threshold than 10k for pending tasks then?).
     * Otherwise a thread pool with a higher size than cpu count should be used that allows IO/sleep between response
     * work processing.
     */
    protected WrappedExecutorService newWorkExecutor() {
        return DEFAULT_WORK_EXECUTOR;
    }

    public final WrappedExecutorService getIoExecutor() {
        return ioExecutor;
    }

    @Override
    public final WrappedExecutorService getWorkExecutor() {
        return workExecutor;
    }

    @Override
    public final int getMaxPendingWorkCountOverall() {
        return maxPendingWorkCountOverall;
    }

    @Override
    public final int getMaxPendingWorkCountPerSession() {
        return maxPendingWorkCountPerSession;
    }

    public final Duration getRequestTimeout() {
        return requestTimeout;
    }

    public final Duration getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    @Override
    public synchronized void open() throws IOException {
        if (ioRunnables != null) {
            throw new IllegalStateException("already opened");
        }
        serverAcceptor.open();
        ioRunnables = ILockCollectionFactory.getInstance(false).newFastIterableArrayList(maxIoThreadCount);
        //main runnable adds more threads on demand
        final IoRunnable rootIoRunnable = new IoRunnable(ROOT_IO_RUNNABLE_ID);
        final ListenableFuture<?> future = getIoExecutor().submit(rootIoRunnable);
        rootIoRunnable.setFuture(future);
        ioRunnables.add(rootIoRunnable);
    }

    @Override
    public synchronized void close() throws IOException {
        if (ioRunnables != null) {
            for (int i = 0; i < ioRunnables.size(); i++) {
                final IoRunnable ioRunnable = ioRunnables.get(i);
                ioRunnable.close();
            }
            ioRunnables = null;
            serverAcceptor.close();
            onClose();
        }
    }

    protected void onClose() {}

    protected abstract ISynchronousEndpointServerSession newServerSession(ISynchronousEndpointSession endpointSession);

    private final class IoRunnable implements Runnable, Closeable {
        private final IFastIterableList<ISynchronousEndpointServerSession> serverSessions = ILockCollectionFactory
                .getInstance(maxIoThreadCount > 1)
                .newFastIterableArrayList();
        private final LoopInterruptedCheck requestWaitLoopInterruptedCheck = new LoopInterruptedCheck(
                requestWaitInterval);
        private final LoopInterruptedCheck heartbeatLoopInterruptedCheck = new LoopInterruptedCheck(heartbeatInterval);
        private final ASpinWait throttle = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                //throttle while nothing to do, spin quickly while work is available
                boolean handledOverall = false;
                boolean handledNow;
                do {
                    handledNow = false;
                    final ISynchronousEndpointServerSession[] serverSessionsArray = serverSessions
                            .asArray(ISynchronousEndpointServerSession.EMPTY_ARRAY);
                    int removedServerSessions = 0;
                    for (int i = 0; i < serverSessionsArray.length; i++) {
                        final ISynchronousEndpointServerSession serverSession = serverSessionsArray[i];
                        try {
                            handledNow |= serverSession.handle();
                            handledOverall |= handledNow;
                        } catch (final EOFException e) {
                            //session closed
                            serverSessions.remove(i - removedServerSessions);
                            activeSessionsOverall.decrementAndGet();
                            Closeables.closeQuietly(serverSession);
                            removedServerSessions++;
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
            return serverSessions.isEmpty() && heartbeatTimeout.isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
        }

        @Override
        public void run() {
            try {
                if (ioRunnableId == ROOT_IO_RUNNABLE_ID) {
                    while (accept()) {
                        process();
                    }
                } else {
                    while (true) {
                        process();
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
                final ISynchronousEndpointServerSession[] serverSessionsArray = serverSessions
                        .asArray(ISynchronousEndpointServerSession.EMPTY_ARRAY);
                for (int i = 0; i < serverSessionsArray.length; i++) {
                    Closeables.closeQuietly(serverSessionsArray[i]);
                }
                serverSessions.clear();
                activeSessionsOverall.addAndGet(-serverSessionsArray.length);
                if (ioRunnableId == ROOT_IO_RUNNABLE_ID) {
                    Closeables.closeQuietly(serverAcceptor);
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
                    if (!throttle.awaitFulfill(System.nanoTime(), requestWaitInterval)) {
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
            final ISynchronousEndpointServerSession[] serverSessionsArray = serverSessions
                    .asArray(ISynchronousEndpointServerSession.EMPTY_ARRAY);
            int removedSessions = 0;
            for (int i = 0; i < serverSessionsArray.length; i++) {
                final ISynchronousEndpointServerSession serverSession = serverSessionsArray[i];
                if (serverSession.isHeartbeatTimeout()) {
                    Err.process(new TimeoutException("Heartbeat timeout [" + serverSession.getHeartbeatTimeout()
                            + "] exceeded: " + serverSession.getSessionId()));
                    serverSessions.remove(i - removedSessions);
                    activeSessionsOverall.decrementAndGet();
                    removedSessions++;
                    Closeables.closeQuietly(serverSession);
                } else if (serverSession.isClosed()) {
                    serverSessions.remove(i - removedSessions);
                    activeSessionsOverall.decrementAndGet();
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
                    heartbeatTimeout = endpointSession.getHeartbeatTimeout();
                    requestTimeout = endpointSession.getRequestTimeout();
                    maybeIncreaseIoRunnableCount();
                    assignServerSessionToIoRunnable(endpointSession);
                } finally {
                    serverAcceptor.readFinished();
                }
            }
            return true;
        }

        private void maybeIncreaseIoRunnableCount() {
            if (ioRunnablesCopy.size() < maxIoThreadCount) {
                int maxIoRunnableId = 0;
                final IoRunnable[] ioRunnablesArray = ioRunnablesCopy.asArray(IO_RUNNABLE_EMPTY_ARRAY);
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
                ioRunnablesCopy.add(newIoRunnable);
            }
        }

        private void assignServerSessionToIoRunnable(final ISynchronousEndpointSession endpointSession) {
            //assign session to the request runnable thread that hsa the least amount of sessions
            int minSessions = Integer.MAX_VALUE;
            int minSessionsIndex = -1;
            final IoRunnable[] ioRunnablesArray = ioRunnablesCopy.asArray(IO_RUNNABLE_EMPTY_ARRAY);
            for (int i = 0; i < ioRunnablesArray.length; i++) {
                final IoRunnable ioRunnable = ioRunnablesCopy.get(i);
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
            ioRunnablesArray[minSessionsIndex].serverSessions.add(newServerSession(endpointSession));
            //update heartbeat because a new session got created
            ioRunnablesArray[minSessionsIndex].lastHeartbeatNanos = System.nanoTime();
            final int activeSessions = activeSessionsOverall.incrementAndGet();
            if (initialMaxPendingWorkCountPerSession < 0) {
                updateMaxPendingCountPerSession(activeSessions);
            }
        }
    }

}
