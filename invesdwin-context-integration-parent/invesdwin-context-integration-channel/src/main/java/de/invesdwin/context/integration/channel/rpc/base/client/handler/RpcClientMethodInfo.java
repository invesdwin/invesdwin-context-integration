package de.invesdwin.context.integration.channel.rpc.base.client.handler;

import java.lang.reflect.Method;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.DisabledUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.IUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.LoggingDelegateUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import de.invesdwin.util.marshallers.serde.lookup.response.IResponseSerdeProvider;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@Immutable
public final class RpcClientMethodInfo implements IServiceMethodInfo {

    private static final Log LOG = new Log(RpcClientMethodInfo.class);
    private static final IUnexpectedMessageListener UNEXPECTED_MESSAGE_LISTENER = new LoggingDelegateUnexpectedMessageListener(
            DisabledUnexpectedMessageListener.INSTANCE, LOG);

    private final RpcSynchronousEndpointClientHandler handler;
    private final int methodId;
    private final ISerde<Object[]> requestSerde;
    private final IResponseSerdeProvider responseSerdeProvider;
    private final boolean blocking;
    private final boolean future;

    RpcClientMethodInfo(final RpcSynchronousEndpointClientHandler handler, final Method method,
            final SerdeLookupConfig serdeLookupConfig) {
        this.handler = handler;
        this.methodId = RpcSynchronousEndpointService.newMethodId(method);
        this.requestSerde = serdeLookupConfig.getRequestLookup().lookup(method);
        this.responseSerdeProvider = serdeLookupConfig.getResponseLookup().lookup(method);
        this.blocking = RpcSynchronousEndpointService.isBlocking(method, true);
        this.future = Future.class.isAssignableFrom(method.getReturnType());
    }

    @Override
    public int getServiceId() {
        return handler.getServiceId();
    }

    @Override
    public int getMethodId() {
        return methodId;
    }

    public boolean isBlocking() {
        return blocking;
    }

    public boolean isFuture() {
        return future;
    }

    public Object invoke(final Object[] args) {
        try (ICloseableByteBuffer buffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            final int argsSize = requestSerde.toBuffer(buffer, args);
            try (ICloseableByteBufferProvider response = request(buffer.sliceTo(argsSize))) {
                final ISerde<Object> responseSerde = responseSerdeProvider.getSerde(args);
                final Object result = responseSerde.fromBuffer(response);
                return result;
            }
        }
    }

    private ICloseableByteBufferProvider request(final IByteBufferProvider request) {
        final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool = handler.getClient()
                .getSessionPool();
        final ISynchronousEndpointClientSession session = sessionPool.borrowObject();
        try {
            return session.request(getServiceId(), getMethodId(), request, session.nextRequestSequence(),
                    session.getDefaultRequestTimeout(), UNEXPECTED_MESSAGE_LISTENER);
        } catch (final TimeoutException e) {
            sessionPool.invalidateObject(session);
            throw new RetryLaterRuntimeException(e);
        } catch (final Throwable t) {
            sessionPool.invalidateObject(session);
            throw Throwables.propagate(t);
        }
    }

}