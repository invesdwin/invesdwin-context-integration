package de.invesdwin.context.integration.channel.rpc.server.async.poll;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;

@Immutable
public final class BlockingPollingQueueProvider implements IPollingQueueProvider {

    public static final IPollingQueueProvider INSTANCE = new BlockingPollingQueueProvider();

    private BlockingPollingQueueProvider() {}

    @Override
    public void addToPollingQueue(final ProcessResponseResult result) {
        result.awaitDone();
        if (result.isDelayedWriteResponse()) {
            result.getContext().write(result.getResponse().asBuffer());
        }
        result.close();
    }

}
