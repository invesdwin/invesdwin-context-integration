package de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;

public interface ISessionlessSynchronousEndpoint<R, W, O>
        extends ISynchronousEndpoint<R, W>, ISessionlessSynchronousChannel<O> {

}
