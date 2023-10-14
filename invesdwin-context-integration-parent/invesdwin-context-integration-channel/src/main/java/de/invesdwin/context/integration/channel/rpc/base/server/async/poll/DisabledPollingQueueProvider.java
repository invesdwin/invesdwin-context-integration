package de.invesdwin.context.integration.channel.rpc.base.server.async.poll;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;

@Immutable
public final class DisabledPollingQueueProvider implements IPollingQueueProvider {

    public static final IPollingQueueProvider INSTANCE = new DisabledPollingQueueProvider();

    private DisabledPollingQueueProvider() {}

    @Override
    public void addToPollingQueue(final ProcessResponseResult result) {}

}
