package de.invesdwin.context.integration.channel.rpc.base.server.blocking;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.transformer.ISynchronousEndpointSessionFactoryTransformer;
import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.RpcAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.DisabledPollingQueueProvider;

@NotThreadSafe
public abstract class ABlockingEndpointServer implements IAsynchronousChannel {

    private final RpcAsynchronousEndpointServerHandlerFactory handlerFactory;
    private final ISynchronousEndpointSessionFactoryTransformer sessionFactoryTransformer;
    private final int maxPendingWorkCountOverall;

    public ABlockingEndpointServer(final RpcAsynchronousEndpointServerHandlerFactory handlerFactory,
            final ISynchronousEndpointSessionFactoryTransformer endpointSessionTransformer) {
        this.handlerFactory = handlerFactory;
        //polling queue disabled because this is handled in BlockingEndpointServiceHandlerContext directly
        handlerFactory.setPollingQueueProvider(DisabledPollingQueueProvider.INSTANCE);
        this.sessionFactoryTransformer = endpointSessionTransformer;
        this.maxPendingWorkCountOverall = newMaxPendingWorkCountOverall();
        if (maxPendingWorkCountOverall < 0) {
            throw new IllegalArgumentException(
                    "maxPendingWorkCountOverall should not be negative: " + maxPendingWorkCountOverall);
        }
    }

    public ISynchronousEndpointSessionFactoryTransformer getSessionFactoryTransformer() {
        return sessionFactoryTransformer;
    }

    public RpcAsynchronousEndpointServerHandlerFactory getHandlerFactory() {
        return handlerFactory;
    }

    @Override
    public void open() throws IOException {
        handlerFactory.open();
    }

    @Override
    public void close() throws IOException {
        handlerFactory.close();
    }

    /**
     * Further requests will be rejected if the workExecutor has more than that amount of requests pending. Only applies
     * when workExecutor is not null.
     * 
     * return 0 here for unlimited pending work count overall.
     */
    protected int newMaxPendingWorkCountOverall() {
        return ASynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL;
    }

    public int getMaxPendingWorkCountOverall() {
        return maxPendingWorkCountOverall;
    }

}
