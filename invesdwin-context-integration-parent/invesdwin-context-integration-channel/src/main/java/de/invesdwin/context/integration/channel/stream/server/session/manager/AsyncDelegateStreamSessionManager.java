package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;

/**
 * With this class all requests can be made async despite there being no worker thread pool. This is useful if for
 * example a specific service is too slow to be executed within the worker/io thread pool and requires a separate thread
 * pool. In that case the server should have no global worker thread pool and threads pools should be manager per
 * service with this wrapper. Though using a worker thread pool could still help to enforce an upper limit on maximum
 * active requests to prevent overloading the server.
 */
@ThreadSafe
public class AsyncDelegateStreamSessionManager
        implements IStreamSessionManager {

    private final IStreamSessionManager delegate;
    private final WrappedExecutorService executor;

    public AsyncDelegateStreamSessionManager(
            final IStreamSessionManager delegate, final WrappedExecutorService executor) {
        this.delegate = delegate;
        if (executor == delegate.getSession().getParent().getWorkExecutor()) {
            throw new IllegalArgumentException(
                    "executor should not be the workExecutor from the server, this will cause deadlocks due to thread starvations");
        }
        this.executor = executor;
    }

    public IStreamSessionManager getDelegate() {
        return delegate;
    }

    @Override
    public IStreamSynchronousEndpointSession getSession() {
        return delegate.getSession();
    }

    @Override
    public IStreamSynchronousEndpointService getService(final int serviceId) {
        return delegate.getService(serviceId);
    }

    @Override
    public IStreamSynchronousEndpointService getOrCreateService(final int serviceId, final String topic,
            final Map<String, String> parameters) {
        return delegate.getOrCreateService(serviceId, topic, parameters);
    }

    @Override
    public boolean handle() throws IOException {
        return delegate.handle();
    }

    @Override
    public boolean isAlwaysFuturePut() {
        return true;
    }

    @Override
    public Object put(final IStreamSynchronousEndpointService service, final IByteBufferProvider message)
            throws Exception {
        final ICloseableByteBuffer messageCopyBuffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject();
        final IByteBuffer messageBuffer = message.asBuffer();
        messageCopyBuffer.putBytes(0, messageBuffer);
        final IByteBuffer messageCopy = messageCopyBuffer.sliceTo(messageBuffer.capacity());
        return executor.submit(() -> {
            try {
                return delegate.put(service, messageCopy);
            } finally {
                messageCopyBuffer.close();
            }
        });
    }

    @Override
    public boolean isAlwaysFutureSubscribe() {
        return true;
    }

    @Override
    public Object subscribe(final IStreamSynchronousEndpointService service, final Map<String, String> parameters) {
        return executor.submit(() -> delegate.subscribe(service, parameters));
    }

    @Override
    public boolean isAlwaysFutureUnsubscribe() {
        return true;
    }

    @Override
    public Object unsubscribe(final IStreamSynchronousEndpointService service, final Map<String, String> parameters) {
        return executor.submit(() -> delegate.unsubscribe(service, parameters));
    }

    @Override
    public boolean isAlwaysFutureDelete() {
        return true;
    }

    @Override
    public Object delete(final IStreamSynchronousEndpointService service, final Map<String, String> parameters) {
        return executor.submit(() -> delegate.delete(service, parameters));
    }

}
