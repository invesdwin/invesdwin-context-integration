package de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class LoggingDelegateUnexpectedMessageListener implements IUnexpectedMessageListener {

    private static final Log DEFAULT_LOG = new Log(LoggingDelegateUnexpectedMessageListener.class);
    private static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.WARN;

    private final IUnexpectedMessageListener delegate;

    private final Log log;
    private final LogLevel logLevel;

    public LoggingDelegateUnexpectedMessageListener(final IUnexpectedMessageListener delegate) {
        this(delegate, DEFAULT_LOG);
    }

    public LoggingDelegateUnexpectedMessageListener(final IUnexpectedMessageListener delegate, final Log log) {
        this(delegate, log, DEFAULT_LOG_LEVEL);
    }

    public LoggingDelegateUnexpectedMessageListener(final IUnexpectedMessageListener delegate, final Log log,
            final LogLevel logLevel) {
        this.delegate = delegate;
        this.log = log;
        this.logLevel = logLevel;
    }

    @Override
    public boolean onPushedWithoutRequest(final ISynchronousEndpointClientSession session, final int serviceId,
            final int methodId, final int streamSequence, final IByteBufferProvider message) {
        if (logLevel.isEnabled(log)) {
            logLevel.log(log, "onPushedWithoutRequest sessionId=%s serviceId=%s methodId=%s streamSequence=%s",
                    session.getEndpointSession().getSessionId(), serviceId, methodId, streamSequence);
        }
        return delegate.onPushedWithoutRequest(session, serviceId, methodId, streamSequence, message);
    }

    @Override
    public void onUnexpectedResponse(final ISynchronousEndpointClientSession session, final int serviceId,
            final int methodId, final int requestSequence, final IByteBufferProvider message) {
        if (logLevel.isEnabled(log)) {
            logLevel.log(log, "onUnexpectedResponse sessionId=%s serviceId=%s methodId=%s requestSequence=%s",
                    session.getEndpointSession().getSessionId(), serviceId, methodId, requestSequence);
        }
        delegate.onUnexpectedResponse(session, serviceId, methodId, requestSequence, message);
    }

}
