package de.invesdwin.context.integration.channel.rpc.base.server;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.concurrent.WrappedExecutorService;

public interface ISynchronousEndpointServer extends ISynchronousChannel {

    WrappedExecutorService getWorkExecutor();

    int getMaxPendingWorkCountOverall();

    int getMaxPendingWorkCountPerSession();

}
