package de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class LoggingDelegateUnexpectedMessageListener implements IUnexpectedMessageListener {

    public static final Log DEFAULT_LOG = new Log(LoggingDelegateUnexpectedMessageListener.class);
    public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.WARN;

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
            final int methodId, final int streamSequence, final IByteBufferProvider message)
            throws AbortRequestException {
        if (logLevel.isEnabled(log)) {
            logLevel.log(log,
                    "onPushedWithoutRequest sessionId=%s serviceId=%s methodId=%s streamSequence=%s message=%s",
                    session.getEndpointSession().getSessionId(), serviceId, methodId, streamSequence,
                    messageToString(methodId, message));
        }
        return delegate.onPushedWithoutRequest(session, serviceId, methodId, streamSequence, message);
    }

    @Override
    public void onUnexpectedResponse(final ISynchronousEndpointClientSession session, final int serviceId,
            final int methodId, final int requestSequence, final IByteBufferProvider message)
            throws AbortRequestException {
        if (logLevel.isEnabled(log)) {
            logLevel.log(log,
                    "onUnexpectedResponse sessionId=%s serviceId=%s methodId=%s requestSequence=%s message=%s",
                    session.getEndpointSession().getSessionId(), serviceId, methodId, requestSequence,
                    messageToString(methodId, message));
        }
        delegate.onUnexpectedResponse(session, serviceId, methodId, requestSequence, message);
    }

    public static String messageToString(final int methodId, final IByteBufferProvider message) {
        final IByteBuffer messageBuffer;
        try {
            messageBuffer = message.asBuffer();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        if (methodId == IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID) {
            final String messageStr = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
            return new RetryLaterRuntimeException(new RemoteExecutionException(messageStr)).toString();
        } else if (methodId == IServiceSynchronousCommand.ERROR_METHOD_ID) {
            final String messageStr = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
            return new RemoteExecutionException(messageStr).toString();
        } else {
            return "[" + messageBuffer.capacity() + "]";
        }
    }

    public static RuntimeException maybeExtractException(final int methodId, final IByteBufferProvider message) {
        try {
            if (methodId == IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID) {
                final IByteBuffer messageBuffer = message.asBuffer();
                final String messageStr = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                return new RetryLaterRuntimeException(new RemoteExecutionException(messageStr));
            } else if (methodId == IServiceSynchronousCommand.ERROR_METHOD_ID) {
                final IByteBuffer messageBuffer = message.asBuffer();
                final String messageStr = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                return new RemoteExecutionException(messageStr);
            } else {
                return null;
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
