package de.invesdwin.context.integration.channel.stream.server.service.log;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.log.LogLevel;

@ThreadSafe
public class LoggingDelegateStreamSynchronousEndpointServiceFactory
        implements IStreamSynchronousEndpointServiceFactory {

    public static final Log DEFAULT_LOG = new Log(LoggingDelegateStreamSynchronousEndpointServiceFactory.class);
    public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

    private final IStreamSynchronousEndpointServiceFactory delegateFactory;

    private final Log log;
    private final LogLevel logLevel;
    private final String id;

    public LoggingDelegateStreamSynchronousEndpointServiceFactory(
            final IStreamSynchronousEndpointServiceFactory delegate) {
        this(delegate, DEFAULT_LOG);
    }

    public LoggingDelegateStreamSynchronousEndpointServiceFactory(
            final IStreamSynchronousEndpointServiceFactory delegate, final Log log) {
        this(delegate, log, DEFAULT_LOG_LEVEL);
    }

    public LoggingDelegateStreamSynchronousEndpointServiceFactory(
            final IStreamSynchronousEndpointServiceFactory delegate, final Log log, final LogLevel logLevel) {
        this.delegateFactory = delegate;
        this.log = log;
        this.logLevel = logLevel;
        this.id = newId(delegate);
    }

    protected String newId(final IStreamSynchronousEndpointServiceFactory delegate) {
        return delegate.toString();
    }

    @Override
    public IStreamSynchronousEndpointService newService(final int serviceId, final String topic,
            final IProperties properties) {
        logLevel.log(log, "%s.newService(%s, %s, %s)", id, serviceId, topic, properties.asMap());
        final IStreamSynchronousEndpointService delegate = delegateFactory.newService(serviceId, topic, properties);
        return new LoggingDelegateStreamSynchronousEndpointService(delegate);
    }

}
