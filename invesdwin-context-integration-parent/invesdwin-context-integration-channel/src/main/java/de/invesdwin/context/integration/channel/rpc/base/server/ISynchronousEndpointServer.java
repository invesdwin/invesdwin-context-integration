package de.invesdwin.context.integration.channel.rpc.base.server;

import java.io.Closeable;
import java.io.IOException;

import de.invesdwin.util.concurrent.WrappedExecutorService;

public interface ISynchronousEndpointServer extends Closeable {

    void open() throws IOException;

    WrappedExecutorService getWorkExecutor();

    int getMaxPendingWorkCountOverall();

    int getMaxPendingWorkCountPerSession();

}
