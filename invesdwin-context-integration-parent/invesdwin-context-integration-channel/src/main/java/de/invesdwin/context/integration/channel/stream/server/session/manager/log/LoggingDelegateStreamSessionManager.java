package de.invesdwin.context.integration.channel.stream.server.session.manager.log;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class LoggingDelegateStreamSessionManager implements IStreamSessionManager {

    public static final Log DEFAULT_LOG = new Log(LoggingDelegateStreamSessionManager.class);
    public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

    private final IStreamSessionManager delegate;

    private final Log log;
    private final LogLevel logLevel;
    private final String id;

    public LoggingDelegateStreamSessionManager(final IStreamSessionManager delegate) {
        this(delegate, DEFAULT_LOG);
    }

    public LoggingDelegateStreamSessionManager(final IStreamSessionManager delegate, final Log log) {
        this(delegate, log, DEFAULT_LOG_LEVEL);
    }

    public LoggingDelegateStreamSessionManager(final IStreamSessionManager delegate, final Log log,
            final LogLevel logLevel) {
        this.delegate = delegate;
        this.log = log;
        this.logLevel = logLevel;
        this.id = newId(delegate);
    }

    protected String newId(final IStreamSessionManager delegate) {
        return delegate.toString();
    }

    @Override
    public void close() throws IOException {
        logLevel.log(log, "%s: close()", id);
        delegate.close();
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
            final IProperties parameters) throws IOException {
        return delegate.getOrCreateService(serviceId, topic, parameters);
    }

    @Override
    public boolean handle() throws IOException {
        return delegate.handle();
    }

    @Override
    public boolean isAlwaysFuturePut() {
        return delegate.isAlwaysFuturePut();
    }

    @Override
    public Object put(final IStreamSynchronousEndpointService service, final IByteBufferProvider message)
            throws Exception {
        logLevel.log(log, "%s: put(%s, ...)", id, service);
        return delegate.put(service, message);
    }

    @Override
    public boolean isAlwaysFutureSubscribe() {
        return delegate.isAlwaysFutureSubscribe();
    }

    @Override
    public Object subscribe(final IStreamSynchronousEndpointService service, final IProperties parameters)
            throws Exception {
        logLevel.log(log, "%s: subscribe(%s, %s)", id, parameters.asMap());
        return delegate.subscribe(service, parameters);
    }

    @Override
    public boolean isAlwaysFutureUnsubscribe() {
        return delegate.isAlwaysFutureUnsubscribe();
    }

    @Override
    public Object unsubscribe(final IStreamSynchronousEndpointService service, final IProperties parameters)
            throws Exception {
        logLevel.log(log, "%s: unsubscribe(%s, %s)", id, parameters.asMap());
        return delegate.unsubscribe(service, parameters);
    }

    @Override
    public boolean isAlwaysFutureDelete() {
        return delegate.isAlwaysFutureDelete();
    }

    @Override
    public Object delete(final IStreamSynchronousEndpointService service, final IProperties parameters)
            throws Exception {
        logLevel.log(log, "%s: delete(%s, %s)", id, parameters.asMap());
        return delegate.delete(service, parameters);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(delegate).toString();
    }

}
