package de.invesdwin.context.integration.channel.rpc.server.blocking.context;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.endpoint.blocking.BlockingSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
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
    private UnsafeByteBuffer requestWrapperBuffer;
    private final ProcessResponseResult result = new ProcessResponseResult();
    private final BlockingSynchronousEndpoint endpoint;
    private final ISynchronousEndpointSession endpointSession;
    private final SynchronousReaderSpinWait<IByteBufferProvider> requestReaderSpinWait;
    private final SynchronousWriterSpinWait<IByteBufferProvider> responseWriterSpinWait;
    private boolean resultBorrowed;
    private AttributesMap attributes;
    private IByteBufferProvider response;

    public BlockingSychrounousEndpointServiceHandlerContext(
            final BlockingSychrounousEndpointServiceHandlerContextPool pool, final BlockingSynchronousEndpoint endpoint,
            final ISynchronousEndpointSession endpointSession,
            final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler) {
        this.pool = pool;
        this.endpoint = endpoint;
        this.endpointSession = endpointSession;
        this.handler = handler;
        this.requestReaderSpinWait = new SynchronousReaderSpinWait<>(endpointSession.newRequestReader());
        this.responseWriterSpinWait = new SynchronousWriterSpinWait<>(endpointSession.newResponseWriter());
    }

    public UnsafeByteBuffer getRequestWrapperBuffer() {
        if (requestWrapperBuffer == null) {
            requestWrapperBuffer = new UnsafeByteBuffer(Bytes.EMPTY_ARRAY);
        }
        return requestWrapperBuffer;
    }

    public void handle(final IByteBufferProvider request) throws IOException {
        endpoint.getReader().getReference().set(request);
        try {
            final IByteBufferProvider transformedRequest = requestReaderSpinWait.spinForRead();
            final IByteBufferProvider output = handler.handle(this, transformedRequest);
            if (output != null) {
                try {
                    write(output);
                } finally {
                    /*
                     * WARNING: this might cause problems if the handler reuses output buffers, since we don't make a
                     * safe copy here for the write queue and further requests could come in. This needs to be
                     * considered when modifying/wrapping the handler. To fix the issue, ProcessResponseResult (via
                     * context.borrowResult() and result.close()) should be used by the handler.
                     */
                    handler.outputFinished(this);
                }
            }
        } finally {
            requestReaderSpinWait.getReader().readFinished();
        }
    }

    @Override
    public String getSessionId() {
        return endpointSession.getSessionId();
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
        try {
            responseWriterSpinWait.spinForWrite(output);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        response = endpoint.getWriter().getReference().getAndSet(null);
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
        requestWrapperBuffer.wrap(Bytes.EMPTY_ARRAY);
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
