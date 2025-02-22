package de.invesdwin.context.integration.channel.rpc.base.server.sessionless.context;

import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResultPool;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.lang.BroadcastingCloseable;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public final class ImmutableSessionlessHandlerContext extends BroadcastingCloseable
        implements ISessionlessHandlerContext {
    private final Object otherSocketAddress;
    private final ManyToOneConcurrentLinkedQueue<MutableSessionlessHandlerContext> writeQueue;
    private AttributesMap attributes;
    private String sessionId;

    public ImmutableSessionlessHandlerContext(final Object otherSocketAddress,
            final ManyToOneConcurrentLinkedQueue<MutableSessionlessHandlerContext> writeQueue) {
        Assertions.checkNotNull(otherSocketAddress);
        this.otherSocketAddress = otherSocketAddress;
        this.writeQueue = writeQueue;
    }

    @Override
    public Object getOtherSocketAddress() {
        return otherSocketAddress;
    }

    @Override
    public Future<?> write(final IByteBufferProvider output) {
        final MutableSessionlessHandlerContext pooledContext = MutableSessionlessHandlerContextPool.INSTANCE
                .borrowObject();
        pooledContext.init(otherSocketAddress, writeQueue);
        return pooledContext.write(output);
    }

    @Override
    public void close() {
        super.close();
        if (attributes != null && !attributes.isEmpty()) {
            attributes.clear();
        }
    }

    @Override
    public String getSessionId() {
        if (sessionId == null) {
            sessionId = otherSocketAddress.toString();
        }
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

    @Override
    public ProcessResponseResult borrowResult() {
        return ProcessResponseResultPool.INSTANCE.borrowObject();
    }

    @Override
    public void returnResult(final ProcessResponseResult result) {
        ProcessResponseResultPool.INSTANCE.returnObject(result);
    }

    @Override
    public IAsynchronousHandlerContext<IByteBufferProvider> asImmutable() {
        return this;
    }

    @Override
    public String toString() {
        return getSessionId();
    }

}