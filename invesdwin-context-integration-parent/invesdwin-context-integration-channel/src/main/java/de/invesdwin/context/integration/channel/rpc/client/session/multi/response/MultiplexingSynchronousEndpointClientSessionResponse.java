package de.invesdwin.context.integration.channel.rpc.client.session.multi.response;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient.ClientMethodInfo;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@ThreadSafe
public class MultiplexingSynchronousEndpointClientSessionResponse implements ICloseableByteBufferProvider {

    private final IObjectPool<MultiplexingSynchronousEndpointClientSessionResponse> pool;
    private final ASpinWait completedSpinWait;
    private ClientMethodInfo methodInfo;
    private IByteBufferProvider request;
    private int requestSequence;
    private AtomicBoolean activePolling;
    private volatile boolean completed;
    private final IByteBuffer response = ByteBuffers.allocateDirectExpandable();
    private int responseSize;
    private long waitingSinceNanos;
    private RuntimeException exceptionResponse;
    private volatile boolean outerActive;
    private volatile boolean pollingActive;
    private volatile boolean writingActive;

    public MultiplexingSynchronousEndpointClientSessionResponse(
            final IObjectPool<MultiplexingSynchronousEndpointClientSessionResponse> pool) {
        this.pool = pool;
        this.completedSpinWait = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return completed || !activePolling.get();
            }
        };
    }

    public synchronized void setOuterActive() {
        assert !this.outerActive : "outerActive should be false";
        this.outerActive = true;
    }

    public boolean isOuterActive() {
        return outerActive;
    }

    public synchronized void setPollingActive() {
        assert !this.pollingActive : "pollingActive should be false";
        assert this.outerActive : "outerActive should be true";
        this.pollingActive = true;
    }

    public boolean isPollingActive() {
        return pollingActive;
    }

    public synchronized void setWritingActive() {
        assert !this.writingActive : "writingActive should be false";
        assert this.outerActive : "outerActive should be true";
        this.writingActive = true;
    }

    public boolean isWritingActive() {
        return writingActive;
    }

    public void init(final ClientMethodInfo methodInfo, final IByteBufferProvider request, final int requestSequence,
            final AtomicBoolean activePolling) {
        this.methodInfo = methodInfo;
        this.request = request;
        this.requestSequence = requestSequence;
        this.activePolling = activePolling;
        this.waitingSinceNanos = System.nanoTime();
    }

    public ClientMethodInfo getMethodInfo() {
        return methodInfo;
    }

    public IByteBufferProvider getRequest() {
        return request;
    }

    public int getRequestSequence() {
        return requestSequence;
    }

    public boolean isCompleted() {
        return completed;
    }

    public ASpinWait getCompletedSpinWait() {
        return completedSpinWait;
    }

    public void responseCompleted(final IByteBufferProvider response) throws IOException {
        responseSize = response.getBuffer(this.response);
        completed = true;
    }

    public void responseCompleted(final RuntimeException exceptionResponse) {
        this.exceptionResponse = exceptionResponse;
        completed = true;
    }

    public long getWaitingSinceNanos() {
        return waitingSinceNanos;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (outerActive) {
                outerActive = false;
                if (!pollingActive && !writingActive) {
                    pool.returnObject(this);
                }
            }
        }
    }

    public void releasePollingActive() {
        synchronized (this) {
            if (pollingActive) {
                pollingActive = false;
                if (!outerActive && !writingActive) {
                    pool.returnObject(this);
                }
            }
        }
    }

    public void releaseWritingActive() {
        synchronized (this) {
            if (writingActive) {
                request = null;
                writingActive = false;
                if (!outerActive && !pollingActive) {
                    pool.returnObject(this);
                }
            }
        }
    }

    public void clean() {
        methodInfo = null;
        request = null;
        requestSequence = 0;
        completed = false;
        responseSize = 0;
        activePolling = null;
        waitingSinceNanos = 0;
        exceptionResponse = null;
        outerActive = false;
        pollingActive = false;
        writingActive = false;
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        if (exceptionResponse != null) {
            throw exceptionResponse;
        }
        response.getBytesTo(0, dst, responseSize);
        return responseSize;
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        if (exceptionResponse != null) {
            throw exceptionResponse;
        }
        return response.sliceTo(responseSize);
    }

}
