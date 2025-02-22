package de.invesdwin.context.integration.channel.stream.client;

import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class LoggingDelegateStreamSynchronousEndpointClient implements IStreamSynchronousEndpointClient {

    public static final Log DEFAULT_LOG = new Log(LoggingDelegateStreamSynchronousEndpointClient.class);
    public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

    private final IStreamSynchronousEndpointClient delegate;

    private final Log log;
    private final LogLevel logLevel;
    private final String id;

    public LoggingDelegateStreamSynchronousEndpointClient(final IStreamSynchronousEndpointClient delegate) {
        this(delegate, DEFAULT_LOG);
    }

    public LoggingDelegateStreamSynchronousEndpointClient(final IStreamSynchronousEndpointClient delegate,
            final Log log) {
        this(delegate, log, DEFAULT_LOG_LEVEL);
    }

    public LoggingDelegateStreamSynchronousEndpointClient(final IStreamSynchronousEndpointClient delegate,
            final Log log, final LogLevel logLevel) {
        this.delegate = delegate;
        this.log = log;
        this.logLevel = logLevel;
        this.id = newId(delegate);
    }

    protected String newId(final IStreamSynchronousEndpointClient delegate) {
        return delegate.toString();
    }

    @Override
    public void open() throws IOException {
        logLevel.log(log, "%s: open()", id);
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        logLevel.log(log, "%s: close()", id);
        delegate.close();
    }

    @Override
    public boolean poll(final Duration timeout) {
        return delegate.poll(timeout);
    }

    @Override
    public Future<?> put(final int serviceId, final ICloseableByteBufferProvider message) {
        logLevel.log(log, "%s: put(%s, ...)", id, serviceId);
        return delegate.put(serviceId, message);
    }

    @Override
    public Future<?> create(final int serviceId, final String topicUri) {
        logLevel.log(log, "%s: create(%s, %s)", id, serviceId, topicUri);
        return delegate.create(serviceId, topicUri);
    }

    @Override
    public Future<?> subscribe(final int serviceId, final String topicUri,
            final IStreamSynchronousEndpointClientSubscription subscription) {
        logLevel.log(log, "%s: subscribe(%s, %s, ...)", id, serviceId, topicUri);
        return delegate.subscribe(serviceId, topicUri, subscription);
    }

    @Override
    public Future<?> unsubscribe(final int serviceId, final String topicUri) {
        logLevel.log(log, "%s: unsubscribe(%s, %s)", id, serviceId, topicUri);
        return delegate.unsubscribe(serviceId, topicUri);
    }

    @Override
    public Future<?> delete(final int serviceId, final String topicUri) {
        logLevel.log(log, "%s: delete(%s, %s)", id, serviceId, topicUri);
        return delegate.delete(serviceId, topicUri);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(delegate).toString();
    }

}
