package de.invesdwin.context.integration.channel.rpc.base.server.blocking;

import java.io.Closeable;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.server.blocking.context.BlockingEndpointServiceHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.blocking.context.BlockingEndpointServiceHandlerContextPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@Immutable
public class BlockingEndpointService implements Closeable {

    private final BlockingEndpointServiceHandlerContextPool contextPool;

    public BlockingEndpointService(final ABlockingEndpointServer parent) {
        this.contextPool = new BlockingEndpointServiceHandlerContextPool(parent);
    }

    public byte[] call(final byte[] request) throws Exception {
        try (BlockingEndpointServiceHandlerContext context = contextPool.borrowObject()) {
            final UnsafeByteBuffer requestBufferWrapper = context.getRequestWrapperBuffer();
            requestBufferWrapper.wrap(request);
            context.handle(requestBufferWrapper);
            final IByteBufferProvider response = context.getResponse();
            return response.asBuffer().asByteArrayCopy();
        }
    }

    public ICloseableByteBufferProvider call(final IByteBufferProvider request) throws Exception {
        final BlockingEndpointServiceHandlerContext context = contextPool.borrowObject();
        context.handle(request);
        //context needs to be returned to the pool from the outside after extracting result from it
        return context;
    }

    @Override
    public void close() {
        contextPool.clear();
    }

}
