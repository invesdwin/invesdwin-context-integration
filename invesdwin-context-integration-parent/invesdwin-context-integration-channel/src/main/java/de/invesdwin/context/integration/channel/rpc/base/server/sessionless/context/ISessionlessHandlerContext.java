package de.invesdwin.context.integration.channel.rpc.base.server.sessionless.context;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface ISessionlessHandlerContext extends IAsynchronousHandlerContext<IByteBufferProvider> {

    Object getOtherSocketAddress();

}
