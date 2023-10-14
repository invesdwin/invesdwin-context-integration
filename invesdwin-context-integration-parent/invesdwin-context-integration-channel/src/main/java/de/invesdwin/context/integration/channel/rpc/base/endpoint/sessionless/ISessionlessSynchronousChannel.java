package de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

public interface ISessionlessSynchronousChannel<O> extends ISynchronousChannel {

    O getOtherSocketAddress();

    void setOtherSocketAddress(O otherSocketAddress);

}
