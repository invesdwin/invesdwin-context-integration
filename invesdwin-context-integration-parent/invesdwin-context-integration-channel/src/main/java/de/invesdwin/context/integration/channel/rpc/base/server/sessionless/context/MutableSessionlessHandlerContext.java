package de.invesdwin.context.integration.channel.rpc.base.server.sessionless.context;

import java.io.Closeable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.concurrent.future.NullFuture;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public final class MutableSessionlessHandlerContext implements ISessionlessHandlerContext {
    private final ProcessResponseResult result = new ProcessResponseResult();
    private boolean resultBorrowed;
    private AttributesMap attributes;
    private Object otherSocketAddress;
    private IByteBufferProvider response;
    private ManyToOneConcurrentLinkedQueue<MutableSessionlessHandlerContext> writeQueue;

    public void init(final Object otherSocketAddress,
            final ManyToOneConcurrentLinkedQueue<MutableSessionlessHandlerContext> writeQueue) {
        this.otherSocketAddress = otherSocketAddress;
        this.writeQueue = writeQueue;
    }

    public IByteBufferProvider getResponse() {
        if (response == null) {
            throw new IllegalStateException("response should not be null");
        }
        return response;
    }

    public ProcessResponseResult getResult() {
        return result;
    }

    @Override
    public Object getOtherSocketAddress() {
        return otherSocketAddress;
    }

    @Override
    public Future<?> write(final IByteBufferProvider output) {
        if (response != null) {
            throw new IllegalStateException("can only write a single response in this context");
        }
        response = output;
        writeQueue.add(this);
        //no way to determine reliably if the write is finished without allocating additional objects
        return NullFuture.getInstance();
    }

    @Override
    public void close() {
        MutableSessionlessHandlerContextPool.INSTANCE.returnObject(this);
    }

    public void clean() {
        otherSocketAddress = null;
        if (resultBorrowed) {
            result.clean();
            resultBorrowed = false;
        }
        if (attributes != null && !attributes.isEmpty()) {
            attributes.clear();
        }
        response = null;
    }

    @Override
    public String getSessionId() {
        return Objects.toString(otherSocketAddress);
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
    public boolean registerCloseable(final Closeable closeable) {
        throw new UnsupportedOperationException("can not register closeable on a transient context");
    }

    @Override
    public boolean unregisterCloseable(final Closeable closeable) {
        throw new UnsupportedOperationException("can not register closeable on a transient context");
    }

    @Override
    public IAsynchronousHandlerContext<IByteBufferProvider> asImmutable() {
        return new ImmutableSessionlessHandlerContext(otherSocketAddress, writeQueue);
    }

    @Override
    public String toString() {
        return getSessionId();
    }

}