package de.invesdwin.context.integration.channel.rpc.base.server.async.poll;

import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;

public interface IPollingQueueProvider {

    void addToPollingQueue(ProcessResponseResult result);

}
