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
            final UnsafeByteBuffer requestBuffer = context.getRequestBuffer();
            requestBuffer.wrap(request);
            final IByteBufferProvider output = context.getHandler().handle(context, requestBuffer);
            if (output != null) {
                try {
                    context.write(output);
                } finally {
                    /*
                     * WARNING: this might cause problems if the handler reuses output buffers, since we don't make a
                     * safe copy here for the write queue and further requests could come in. This needs to be
                     * considered when modifying/wrapping the handler. To fix the issue, ProcessResponseResult (via
                     * context.borrowResult() and result.close()) should be used by the handler.
                     */
                    context.getHandler().outputFinished(context);
                }
            }
            final IByteBufferProvider response = context.getResponse();
            return response.asBuffer().asByteArrayCopy();
        }
    }

    public ICloseableByteBufferProvider call(final IByteBufferProvider request) throws Exception {
        final BlockingSychrounousEndpointServiceHandlerContext context = contextPool.borrowObject();
        final IByteBufferProvider output = context.getHandler().handle(context, request);
        if (output != null) {
            try {
                context.write(output);
            } finally {
                /*
                 * WARNING: this might cause problems if the handler reuses output buffers, since we don't make a safe
                 * copy here for the write queue and further requests could come in. This needs to be considered when
                 * modifying/wrapping the handler. To fix the issue, ProcessResponseResult (via context.borrowResult()
                 * and result.close()) should be used by the handler.
                 */
                context.getHandler().outputFinished(context);
            }
        }
        //context needs to be returned to the pool from the outside after extracting result from it
        return context;
    }

    @Override
    public void close() {
        contextPool.clear();
    }

}
