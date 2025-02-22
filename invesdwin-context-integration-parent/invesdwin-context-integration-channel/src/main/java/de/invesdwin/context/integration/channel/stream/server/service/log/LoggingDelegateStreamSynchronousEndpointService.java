package de.invesdwin.context.integration.channel.stream.server.service.log;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceListener;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class LoggingDelegateStreamSynchronousEndpointService implements IStreamSynchronousEndpointService {

    public static final Log DEFAULT_LOG = new Log(LoggingDelegateStreamSynchronousEndpointService.class);
    public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

    private final IStreamSynchronousEndpointService delegate;

    private final Log log;
    private final LogLevel logLevel;
    private final String id;

    public LoggingDelegateStreamSynchronousEndpointService(final IStreamSynchronousEndpointService delegate) {
        this(delegate, DEFAULT_LOG);
    }

    public LoggingDelegateStreamSynchronousEndpointService(final IStreamSynchronousEndpointService delegate,
            final Log log) {
        this(delegate, log, DEFAULT_LOG_LEVEL);
    }

    public LoggingDelegateStreamSynchronousEndpointService(final IStreamSynchronousEndpointService delegate,
            final Log log, final LogLevel logLevel) {
        this.delegate = delegate;
        this.log = log;
        this.logLevel = logLevel;
        this.id = newId(delegate);
    }

    protected String newId(final IStreamSynchronousEndpointService delegate) {
        return delegate.toString();
    }

    @Override
    public void close() throws IOException {
        logLevel.log(log, "%s.close()", id);
        delegate.close();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public void open() throws IOException {
        logLevel.log(log, "%s.open()", id);
        delegate.open();
    }

    @Override
    public int getServiceId() {
        return delegate.getServiceId();
    }

    @Override
    public String getTopic() {
        return delegate.getTopic();
    }

    @Override
    public boolean put(final IByteBufferProvider message) throws Exception {
        logLevel.log(log, "%s.put(...)", id);
        return delegate.put(message);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> subscribe(final IStreamSynchronousEndpointServiceListener listener,
            final IProperties parameters) {
        logLevel.log(log, "%s.subscribe(..., %s)", id, parameters.asMap());
        return delegate.subscribe(listener, parameters);
    }

    @Override
    public boolean unsubscribe(final IStreamSynchronousEndpointServiceListener listener, final IProperties parameters) {
        logLevel.log(log, "%s.unsubscribe(..., %s)", id, parameters.asMap());
        return delegate.unsubscribe(listener, parameters);
    }

    @Override
    public boolean delete(final IProperties parameters) throws Exception {
        logLevel.log(log, "%s.delete(..., %s)", id, parameters.asMap());
        return delegate.delete(parameters);
    }

}
