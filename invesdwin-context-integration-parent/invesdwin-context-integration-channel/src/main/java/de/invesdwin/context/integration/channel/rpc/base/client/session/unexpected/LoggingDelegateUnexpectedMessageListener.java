package de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class LoggingDelegateUnexpectedMessageListener implements IUnexpectedMessageListener {

    private static final Log LOG = new Log(LoggingDelegateUnexpectedMessageListener.class);

    private final IUnexpectedMessageListener delegate;

    private final Log log;

    public LoggingDelegateUnexpectedMessageListener(final IUnexpectedMessageListener delegate) {
        this(delegate, LOG);
    }

    public LoggingDelegateUnexpectedMessageListener(final IUnexpectedMessageListener delegate, final Log log) {
        this.delegate = delegate;
        this.log = log;
    }

    @Override
    public boolean onPushedWithoutRequest(final ISynchronousEndpointClientSession session, final int serviceId,
            final int methodId, final int streamSequence, final IByteBufferProvider message) {
        log.warn("onPushedWithoutRequest sessionId=%s serviceId=%s methodId=%s streamSequence=%s",
                session.getEndpointSession().getSessionId(), serviceId, methodId, streamSequence);
        return delegate.onPushedWithoutRequest(session, serviceId, methodId, streamSequence, message);
    }

    @Override
    public void onUnexpectedResponse(final ISynchronousEndpointClientSession session, final int serviceId,
            final int methodId, final int requestSequence, final IByteBufferProvider message) {
        log.warn("onUnexpectedResponse sessionId=%s serviceId=%s methodId=%s requestSequence=%s",
                session.getEndpointSession().getSessionId(), serviceId, methodId, requestSequence);
        delegate.onUnexpectedResponse(session, serviceId, methodId, requestSequence, message);
    }

}
