package de.invesdwin.context.integration.channel.rpc.server.handler.poll;

import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;

public interface IPollingQueueProvider {

    void addToPollingQueue(ProcessResponseResult result);

}
