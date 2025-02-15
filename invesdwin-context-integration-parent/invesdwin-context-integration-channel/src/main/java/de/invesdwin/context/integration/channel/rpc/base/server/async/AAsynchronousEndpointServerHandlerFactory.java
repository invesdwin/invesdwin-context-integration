package de.invesdwin.context.integration.channel.rpc.base.server.async;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.ISynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.AsyncPollingQueueProvider;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.IPollingQueueProvider;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.math.Integers;

@ThreadSafe
public abstract class AAsynchronousEndpointServerHandlerFactory
        implements IAsynchronousEndpointServerHandlerFactory, ISynchronousEndpointServer {

    public static final int DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL = ASynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL;
    public static final int DEFAULT_MAX_PENDING_WORK_COUNT_PER_SESSION = Integers
            .abs(ASynchronousEndpointServer.DEFAULT_INITIAL_MAX_PENDING_WORK_COUNT_PER_SESSION);
    public static final WrappedExecutorService DEFAULT_WORK_EXECUTOR = ASynchronousEndpointServer.DEFAULT_WORK_EXECUTOR;
    private final WrappedExecutorService workExecutor;
    private final int maxPendingWorkCountOverall;
    private final int maxPendingWorkCountPerSession;
    private IPollingQueueProvider pollingQueueProvider = AsyncPollingQueueProvider.INSTANCE;

    public AAsynchronousEndpointServerHandlerFactory() {
        this.workExecutor = newWorkExecutor();
        this.maxPendingWorkCountOverall = newMaxPendingWorkCountOverall();
        if (maxPendingWorkCountOverall < 0) {
            throw new IllegalArgumentException(
                    "maxPendingWorkCountOverall should not be negative: " + maxPendingWorkCountOverall);
        }
        this.maxPendingWorkCountPerSession = newMaxPendingWorkCountPerSession();
        if (maxPendingWorkCountPerSession < 0) {
            throw new IllegalArgumentException(
                    "maxPendingWorkCountPerSession should not be negative: " + maxPendingWorkCountPerSession);
        }
    }

    @Override
    public void setPollingQueueProvider(final IPollingQueueProvider pollingQueueProvider) {
        this.pollingQueueProvider = pollingQueueProvider;
    }

    @Override
    public IPollingQueueProvider getPollingQueueProvider() {
        return pollingQueueProvider;
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
    protected int newMaxPendingWorkCountPerSession() {
        return DEFAULT_MAX_PENDING_WORK_COUNT_PER_SESSION;
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

    @Override
    public final WrappedExecutorService getWorkExecutor() {
        return workExecutor;
    }

    @Override
    public int getMaxPendingWorkCountOverall() {
        return maxPendingWorkCountOverall;
    }

    @Override
    public int getMaxPendingWorkCountPerSession() {
        return maxPendingWorkCountPerSession;
    }

}
