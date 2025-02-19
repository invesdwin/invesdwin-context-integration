package de.invesdwin.context.integration.channel.stream.client;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class AsyncDelegateSynchronousEndpointClient implements IStreamSynchronousEndpointClient {

    public static final WrappedExecutorService DEFAULT_REQUEST_EXECUTOR = Executors
            .newCachedThreadPool(AsyncDelegateSynchronousEndpointClient.class.getSimpleName() + "_REQUEST");
    /**
     * use only one thread so that order of elements in kept intact
     */
    public static final WrappedExecutorService DEFAULT_PUT_EXECUTOR = Executors
            .newFixedThreadPool(AsyncDelegateSynchronousEndpointClient.class.getSimpleName() + "_PUT", 1);

    private final IStreamSynchronousEndpointClient delegate;
    private final WrappedExecutorService requestExecutor;
    private final WrappedExecutorService putExecutor;

    public AsyncDelegateSynchronousEndpointClient(final IStreamSynchronousEndpointClient delegate) {
        this.delegate = delegate;
        this.requestExecutor = newRequestExecutor();
        this.putExecutor = newPutExecutor();
    }

    protected WrappedExecutorService newRequestExecutor() {
        return DEFAULT_REQUEST_EXECUTOR;
    }

    public WrappedExecutorService getRequestExecutor() {
        return requestExecutor;
    }

    protected WrappedExecutorService newPutExecutor() {
        return DEFAULT_PUT_EXECUTOR;
    }

    public WrappedExecutorService getPutExecutor() {
        return putExecutor;
    }

    @Override
    public int newServiceId(final String topic) {
        return delegate.newServiceId(topic);
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void poll(final Duration timeout) throws TimeoutException {
        delegate.poll(timeout);
    }

    @Override
    public Future<?> put(final int serviceId, final ICloseableByteBufferProvider message) {
        return getPutExecutor().submit(() -> delegate.put(serviceId, message));
    }

    @Override
    public Future<?> create(final int serviceId, final String topicUri) {
        return getRequestExecutor().submit(() -> Futures.get(delegate.create(serviceId, topicUri)));
    }

    @Override
    public Future<?> subscribe(final int serviceId, final String topicUri,
            final IStreamSynchronousEndpointClientSubscription subscription) {
        return getRequestExecutor().submit(() -> Futures.get(delegate.subscribe(serviceId, topicUri, subscription)));
    }

    @Override
    public Future<?> unsubscribe(final int serviceId, final String topicUri) {
        return getRequestExecutor().submit(() -> Futures.get(delegate.unsubscribe(serviceId, topicUri)));
    }

    @Override
    public Future<?> delete(final int serviceId, final String topicUri) {
        return getRequestExecutor().submit(() -> Futures.get(delegate.delete(serviceId, topicUri)));
    }

}
