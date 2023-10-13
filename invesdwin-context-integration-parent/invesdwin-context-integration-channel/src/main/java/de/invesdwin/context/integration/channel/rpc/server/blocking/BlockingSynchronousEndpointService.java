package de.invesdwin.context.integration.channel.rpc.server.blocking;

import java.io.Closeable;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.server.blocking.context.BlockingSychrounousEndpointServiceHandlerContext;
import de.invesdwin.context.integration.channel.rpc.server.blocking.context.BlockingSychrounousEndpointServiceHandlerContextPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@Immutable
public class BlockingSynchronousEndpointService implements Closeable {

    private final BlockingSychrounousEndpointServiceHandlerContextPool contextPool;

    public BlockingSynchronousEndpointService(final ABlockingSynchronousEndpointServer parent) {
        this.contextPool = new BlockingSychrounousEndpointServiceHandlerContextPool(parent);
    }

    public byte[] call(final byte[] request) throws Exception {
        try (BlockingSychrounousEndpointServiceHandlerContext context = contextPool.borrowObject()) {
            final UnsafeByteBuffer requestBufferWrapper = context.getRequestWrapperBuffer();
            requestBufferWrapper.wrap(request);
            context.handle(requestBufferWrapper);
            final IByteBufferProvider response = context.getResponse();
            return response.asBuffer().asByteArrayCopy();
        }
    }

    public ICloseableByteBufferProvider call(final IByteBufferProvider request) throws Exception {
        final BlockingSychrounousEndpointServiceHandlerContext context = contextPool.borrowObject();
        context.handle(request);
        //context needs to be returned to the pool from the outside after extracting result from it
        return context;
    }

    @Override
    public void close() {
        contextPool.clear();
    }

}
