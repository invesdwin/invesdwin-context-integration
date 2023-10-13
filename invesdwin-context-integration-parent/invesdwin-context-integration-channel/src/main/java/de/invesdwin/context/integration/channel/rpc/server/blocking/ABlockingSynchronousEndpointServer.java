package de.invesdwin.context.integration.channel.rpc.server.blocking;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.transformer.DefaultSynchronousEndpointSessionFactoryTransformer;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.transformer.ISynchronousEndpointSessionFactoryTransformer;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.async.poll.BlockingPollingQueueProvider;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

@NotThreadSafe
public abstract class ABlockingSynchronousEndpointServer implements ISynchronousChannel {

    private final AsynchronousEndpointServerHandlerFactory handlerFactory;
    private final ISynchronousEndpointSessionFactoryTransformer sessionFactoryTransformer;
    private final int maxPendingWorkCountOverall;

    public ABlockingSynchronousEndpointServer(final AsynchronousEndpointServerHandlerFactory handlerFactory) {
        this.handlerFactory = handlerFactory;
        handlerFactory.setPollingQueueProvider(BlockingPollingQueueProvider.INSTANCE);
        this.sessionFactoryTransformer = newSessionFactoryTransformer();
        this.maxPendingWorkCountOverall = newMaxPendingWorkCountOverall();
        if (maxPendingWorkCountOverall < 0) {
            throw new IllegalArgumentException(
                    "maxPendingWorkCountOverall should not be negative: " + maxPendingWorkCountOverall);
        }
    }

    /**
     * Could add compression and pre-shared encryption/authentication with this override.
     */
    protected ISynchronousEndpointSessionFactoryTransformer newSessionFactoryTransformer() {
        return new DefaultSynchronousEndpointSessionFactoryTransformer();
    }

    public ISynchronousEndpointSessionFactoryTransformer getSessionFactoryTransformer() {
        return sessionFactoryTransformer;
    }

    public AsynchronousEndpointServerHandlerFactory getHandlerFactory() {
        return handlerFactory;
    }

    /**
     * Further requests will be rejected if the workExecutor has more than that amount of requests pending. Only applies
     * when workExecutor is not null.
     * 
     * return 0 here for unlimited pending work count overall.
     */
    protected int newMaxPendingWorkCountOverall() {
        return SynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL;
    }

    public int getMaxPendingWorkCountOverall() {
        return maxPendingWorkCountOverall;
    }

}
