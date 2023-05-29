package de.invesdwin.context.integration.channel.rpc.server.session.result;

import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.server.service.command.CopyBufferServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator.INode;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ProcessResponseResult implements INode<ProcessResponseResult> {

    public static final ProcessResponseResult[] EMPTY_ARRAY = new ProcessResponseResult[0];

    private final CopyBufferServiceSynchronousCommand requestCopy = new CopyBufferServiceSynchronousCommand();
    private final EagerSerializingServiceSynchronousCommand<Object> response = new EagerSerializingServiceSynchronousCommand<Object>();
    @GuardedBy("only the IO thread has access to the future")
    private Future<Object> future;
    @GuardedBy("only the IO thread has access to this flag")
    private boolean writing;
    private ProcessResponseResult next;
    private ProcessResponseResult prev;
    private boolean delayedWriteResponse;
    private IAsynchronousHandlerContext<IByteBufferProvider> context;

    public CopyBufferServiceSynchronousCommand getRequestCopy() {
        return requestCopy;
    }

    public EagerSerializingServiceSynchronousCommand<Object> getResponse() {
        return response;
    }

    public void setFuture(final Future<Object> future) {
        this.future = future;
    }

    public Future<Object> getFuture() {
        return future;
    }

    public void setWriting(final boolean writing) {
        this.writing = writing;
    }

    public boolean isWriting() {
        return writing;
    }

    public void setDelayedWriteResponse(final boolean delayedWriteResponse) {
        this.delayedWriteResponse = delayedWriteResponse;
    }

    public boolean isDelayedWriteResponse() {
        return delayedWriteResponse;
    }

    public IAsynchronousHandlerContext<IByteBufferProvider> getContext() {
        return context;
    }

    public void setContext(final IAsynchronousHandlerContext<IByteBufferProvider> context) {
        this.context = context;
    }

    @Override
    public ProcessResponseResult getNext() {
        return next;
    }

    @Override
    public void setNext(final ProcessResponseResult next) {
        this.next = next;
    }

    @Override
    public ProcessResponseResult getPrev() {
        return prev;
    }

    @Override
    public void setPrev(final ProcessResponseResult prev) {
        this.prev = prev;
    }

    public void close() {
        future = null;
        writing = false;
        requestCopy.close();
        response.close();
        delayedWriteResponse = false;
        prev = null;
        next = null;
        context = null;
    }

    public boolean isDone() {
        if (future.isDone()) {
            if (future.isCancelled()) {
                delayedWriteResponse = false;
                return true;
            }
            final Future<?> nextFuture = (Future<?>) Futures.getNoInterrupt(future);
            if (nextFuture != null) {
                return nextFuture.isDone();
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

}