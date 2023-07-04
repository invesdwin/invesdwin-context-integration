package de.invesdwin.context.integration.channel.rpc.server.async.sessionless;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;

public interface ISessionlessSynchronousEndpoint<R, W, O> extends ISynchronousEndpoint<R, W> {

    O getOtherRemoteAddress();

    void setOtherRemoteAddress(O otherRemoteAddress);

}
