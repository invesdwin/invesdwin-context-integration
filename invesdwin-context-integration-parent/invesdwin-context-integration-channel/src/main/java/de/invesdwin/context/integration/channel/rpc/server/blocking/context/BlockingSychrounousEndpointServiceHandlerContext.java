package de.invesdwin.context.integration.channel.rpc.server.blocking.context;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class BlockingSychrounousEndpointServiceHandlerContext
        implements IAsynchronousHandlerContext<IByteBufferProvider>, ICloseableByteBufferProvider {

    private final BlockingSychrounousEndpointServiceHandlerContextPool pool;
    private final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
    private UnsafeByteBuffer requestBuffer;
    private final ProcessResponseResult result = new ProcessResponseResult();
    private final String sessionId;
    private boolean resultBorrowed;
    private AttributesMap attributes;
    private IByteBufferProvider response;

    public BlockingSychrounousEndpointServiceHandlerContext(
            final BlockingSychrounousEndpointServiceHandlerContextPool pool, final String sessionId,
            final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler) {
        this.pool = pool;
        this.sessionId = sessionId;
        this.handler = handler;
    }

    public IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> getHandler() {
        return handler;
    }

    public UnsafeByteBuffer getRequestBuffer() {
        if (requestBuffer == null) {
            requestBuffer = new UnsafeByteBuffer(Bytes.EMPTY_ARRAY);
        }
        return requestBuffer;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public AttributesMap getAttributes() {
        if (attributes == null) {
            synchronized (this) {
                if (attributes == null) {
                    attributes = new AttributesMap();
                }
            }
        }
        return attributes;
    }

    public IByteBufferProvider getResponse() {
        if (response == null) {
            throw new IllegalStateException("response should not be null");
        }
        return response;
    }

    @Override
    public void write(final IByteBufferProvider output) {
        if (response != null) {
            throw new IllegalStateException("can only write a single response in this context");
        }
        response = output;
    }

    @Override
    public ProcessResponseResult borrowResult() {
        if (resultBorrowed) {
            throw new IllegalStateException("only one result can be borrowed in this context");
        }
        resultBorrowed = true;
        return result;
    }

    @Override
    public void returnResult(final ProcessResponseResult result) {
        //don't clean yet
    }

    @Override
    public void close() {
        pool.returnObject(this);
    }

    public void clean() {
        requestBuffer.wrap(Bytes.EMPTY_ARRAY);
        response = null;
        if (resultBorrowed) {
            result.clean();
            resultBorrowed = false;
        }
        if (attributes != null && !attributes.isEmpty()) {
            attributes.clear();
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        return getResponse().getBuffer(dst);
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        return getResponse().asBuffer();
    }

}
