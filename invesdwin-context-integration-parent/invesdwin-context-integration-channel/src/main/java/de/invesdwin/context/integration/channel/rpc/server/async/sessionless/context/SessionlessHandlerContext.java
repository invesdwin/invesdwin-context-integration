package de.invesdwin.context.integration.channel.rpc.server.async.sessionless.context;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public final class SessionlessHandlerContext implements IAsynchronousHandlerContext<IByteBufferProvider> {
    private final ProcessResponseResult result = new ProcessResponseResult();
    private AttributesMap attributes;
    private Object otherRemoteAddress;
    private IByteBufferProvider response;
    private ManyToOneConcurrentLinkedQueue<SessionlessHandlerContext> writeQueue;

    public void init(final Object otherRemoteAddress,
            final ManyToOneConcurrentLinkedQueue<SessionlessHandlerContext> writeQueue) {
        this.otherRemoteAddress = otherRemoteAddress;
        this.writeQueue = writeQueue;
    }

    public IByteBufferProvider getResponse() {
        return response;
    }

    public ProcessResponseResult getResult() {
        return result;
    }

    public Object getOtherRemoteAddress() {
        return otherRemoteAddress;
    }

    @Override
    public void write(final IByteBufferProvider output) {
        if (response != null) {
            throw new IllegalStateException("can only write a single response");
        }
        response = output;
        writeQueue.add(this);
    }

    @Override
    public void close() throws IOException {
        SessionlessHandlerContextPool.INSTANCE.returnObject(this);
    }

    public void clean() {
        otherRemoteAddress = null;
        result.clean();
        attributes = null;
        response = null;
    }

    @Override
    public String getSessionId() {
        return Objects.toString(otherRemoteAddress);
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
        return result;
    }

    @Override
    public void returnResult(final ProcessResponseResult result) {
        //don't clean yet
    }

}