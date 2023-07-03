package de.invesdwin.context.integration.channel.rpc.server.async;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;

public interface IAsynchronousEndpoint<R, W, O> extends ISynchronousEndpoint<R, W> {

    O getOtherRemoteAddress();

    void setOtherRemoteAddress(O otherRemoteAddress);

}
