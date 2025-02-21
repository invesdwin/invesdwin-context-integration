package de.invesdwin.context.integration.channel.rpc.base.client.session.multi.response;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.client.handler.IServiceMethodInfo;
import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class MultiplexingSynchronousEndpointClientSessionResponse
        implements ICloseableByteBufferProvider, IServiceMethodInfo {

    private final IObjectPool<MultiplexingSynchronousEndpointClientSessionResponse> pool;
    private final ASpinWait completedSpinWait;
    private int serviceId;
    private int methodId;
    private int requestSequence;
    private IByteBufferProvider request;
    private boolean closeRequest;
    private Duration requestTimeout;
    private AtomicBoolean activePolling;
    private volatile boolean completed;
    private final IByteBuffer response = ByteBuffers.allocateDirectExpandable();
    private int responseSize;
    private long waitingSinceNanos;
    private RuntimeException exceptionResponse;
    private volatile boolean outerActive;
    private volatile boolean pollingActive;
    private volatile boolean writingActive;
    private boolean pushedWithoutRequest;

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

    public void init(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request, final boolean closeRequest, final Duration requestTimeout,
            final AtomicBoolean activePolling) {
        this.serviceId = serviceId;
        this.methodId = methodId;
        this.requestSequence = requestSequence;
        this.request = request;
        this.closeRequest = closeRequest;
        this.requestTimeout = requestTimeout;
        this.activePolling = activePolling;
        this.waitingSinceNanos = System.nanoTime();
    }

    public void setPushedWithoutRequest() {
        this.pushedWithoutRequest = true;
    }

    public boolean isPushedWithoutRequest() {
        return pushedWithoutRequest;
    }

    @Override
    public int getServiceId() {
        return serviceId;
    }

    @Override
    public int getMethodId() {
        return methodId;
    }

    public IByteBufferProvider getRequest() {
        return request;
    }

    public int getRequestSequence() {
        return requestSequence;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public boolean isRequestTimeout() {
        return getRequestTimeout().isLessThanOrEqualToNanos(System.nanoTime() - getWaitingSinceNanos());
    }

    public boolean isCompleted() {
        return completed;
    }

    public ASpinWait getCompletedSpinWait() {
        return completedSpinWait;
    }

    public void responseCompleted(final IByteBufferProvider response) throws IOException {
        this.responseSize = response.getBuffer(this.response);
        this.completed = true;
    }

    public void responseCompleted(final RuntimeException exceptionResponse) {
        this.exceptionResponse = exceptionResponse;
        this.completed = true;
    }

    public void maybeResponseCompleted(
            final MultiplexingSynchronousEndpointClientSessionResponse pushedWithoutRequest) {
        if (!pushedWithoutRequest.completed) {
            return;
        }
        if (pushedWithoutRequest.response != null) {
            this.responseSize = pushedWithoutRequest.response.getBuffer(this.response);
        } else {
            this.exceptionResponse = pushedWithoutRequest.exceptionResponse;
        }
        this.completed = true;
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
                if (closeRequest) {
                    final Closeable cRequest = (Closeable) request;
                    Closeables.closeQuietly(cRequest);
                    closeRequest = false;
                }
                request = null;
                writingActive = false;
                if (!outerActive && !pollingActive) {
                    pool.returnObject(this);
                }
            }
        }
    }

    public void clean() {
        serviceId = 0;
        methodId = 0;
        requestSequence = 0;
        if (closeRequest && request != null) {
            final Closeable cRequest = (Closeable) request;
            Closeables.closeQuietly(cRequest);
        }
        request = null;
        closeRequest = false;
        requestTimeout = null;
        completed = false;
        responseSize = 0;
        activePolling = null;
        waitingSinceNanos = 0;
        exceptionResponse = null;
        outerActive = false;
        pollingActive = false;
        writingActive = false;
        pushedWithoutRequest = false;
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

    public boolean isPollOnly() {
        return serviceId == ISynchronousEndpointClientSession.INVALID_POLL_ONLY_ID
                && methodId == ISynchronousEndpointClientSession.INVALID_POLL_ONLY_ID
                && requestSequence == ISynchronousEndpointClientSession.INVALID_POLL_ONLY_ID;
    }

}
