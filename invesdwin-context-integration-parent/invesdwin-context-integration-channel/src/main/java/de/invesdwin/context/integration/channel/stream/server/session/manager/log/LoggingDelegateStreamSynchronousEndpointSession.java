package de.invesdwin.context.integration.channel.stream.server.session.manager.log;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.IStreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class LoggingDelegateStreamSynchronousEndpointSession implements IStreamSynchronousEndpointSession {

    public static final Log DEFAULT_LOG = new Log(LoggingDelegateStreamSynchronousEndpointSession.class);
    public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

    private final IStreamSynchronousEndpointSession delegate;

    private final Log log;
    private final LogLevel logLevel;
    private final String id;

    public LoggingDelegateStreamSynchronousEndpointSession(final IStreamSynchronousEndpointSession delegate) {
        this(delegate, DEFAULT_LOG);
    }

    public LoggingDelegateStreamSynchronousEndpointSession(final IStreamSynchronousEndpointSession delegate,
            final Log log) {
        this(delegate, log, DEFAULT_LOG_LEVEL);
    }

    public LoggingDelegateStreamSynchronousEndpointSession(final IStreamSynchronousEndpointSession delegate,
            final Log log, final LogLevel logLevel) {
        this.delegate = delegate;
        this.log = log;
        this.logLevel = logLevel;
        this.id = newId(delegate);
    }

    protected String newId(final IStreamSynchronousEndpointSession delegate) {
        return delegate.toString();
    }

    @Override
    public IStreamSynchronousEndpointServer getServer() {
        return delegate.getServer();
    }

    @Override
    public boolean pushSubscriptionMessage(final IStreamSynchronousEndpointService service,
            final ISynchronousReader<IByteBufferProvider> reader) throws IOException {
        logLevel.log(log, "%s.pushSubscriptionMessage(%s, ...)", id, service);
        return delegate.pushSubscriptionMessage(service, reader);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

}
