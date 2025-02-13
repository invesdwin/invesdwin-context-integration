package de.invesdwin.context.integration.channel.stream.server.service;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.rpc.base.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.stream.server.session.IStreamSynchronousEndpointServerSessionInternalMethods;
import de.invesdwin.context.integration.retry.Retries;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.log.error.LoggedRuntimeException;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.future.APostProcessingFuture;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.marshallers.serde.basic.StringUtf8Serde;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import io.netty.util.concurrent.FastThreadLocal;

@Immutable
public enum StreamServerMethodInfo {
    PUT(false) {
        @Override
        public int getMethodId() {
            return METHOD_ID_PUT;
        }

        @Override
        protected Object invoke(final IStreamSynchronousEndpointServerSessionInternalMethods parent,
                final IStreamSynchronousEndpointService service, final IByteBufferProvider message) throws Exception {
            return parent.put(message);
        }
    },
    SUBSCRIBE(true) {
        @Override
        public int getMethodId() {
            return METHOD_ID_SUBSCRIBE;
        }

        @Override
        protected IStreamSynchronousEndpointService getService(
                final IStreamSynchronousEndpointServerSessionInternalMethods parent, final int serviceId,
                final IByteBufferProvider message) throws Exception {
            final IByteBuffer messageBuffer = message.asBuffer();
            if (messageBuffer.capacity() == 0) {
                return parent.getService(serviceId);
            } else {
                //we can also create a topic while subscribing to it if parameters are provided
                final URI uri = parseUri(messageBuffer);
                final String topic = parseTopic(uri);
                final Map<String, String> parameters = parseParameters(uri);
                return parent.getOrCreateService(serviceId, topic, parameters);
            }
        }

        @Override
        protected Object invoke(final IStreamSynchronousEndpointServerSessionInternalMethods parent,
                final IStreamSynchronousEndpointService service, final IByteBufferProvider message) throws Exception {
            return parent.subscribe(service);
        }
    },
    UNSUBSCRIBE(true) {
        @Override
        public int getMethodId() {
            return METHOD_ID_UNSUBSCRIBE;
        }

        @Override
        protected Object invoke(final IStreamSynchronousEndpointServerSessionInternalMethods parent,
                final IStreamSynchronousEndpointService service, final IByteBufferProvider message) throws Exception {
            return parent.unsubscribe(service);
        }
    },
    CREATE(false) {
        @Override
        public int getMethodId() {
            return METHOD_ID_CREATE;
        }

        @Override
        protected IStreamSynchronousEndpointService getService(
                final IStreamSynchronousEndpointServerSessionInternalMethods parent, final int serviceId,
                final IByteBufferProvider message) throws Exception {
            final URI uri = parseUri(message);
            final String topic = parseTopic(uri);
            final Map<String, String> parameters = parseParameters(uri);
            return parent.getOrCreateService(serviceId, topic, parameters);
        }

        @Override
        protected Object invoke(final IStreamSynchronousEndpointServerSessionInternalMethods parent,
                final IStreamSynchronousEndpointService service, final IByteBufferProvider message) throws Exception {
            return null;
        }
    },
    DELETE(false) {
        @Override
        public int getMethodId() {
            return METHOD_ID_DELETE;
        }

        @Override
        protected Object invoke(final IStreamSynchronousEndpointServerSessionInternalMethods parent,
                final IStreamSynchronousEndpointService service, final IByteBufferProvider message) throws Exception {
            return parent.delete(service);
        }
    };

    public static final int METHOD_ID_PUT = 0;
    public static final int METHOD_ID_SUBSCRIBE = 1;
    public static final int METHOD_ID_UNSUBSCRIBE = 2;
    public static final int METHOD_ID_CREATE = 3;
    public static final int METHOD_ID_DELETE = 4;

    private static final FastThreadLocal<Map<String, String>> QUERY_PARAMS_HOLDER = new FastThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String, String> initialValue() throws Exception {
            return new LinkedHashMap<>();
        };
    };

    static {
        for (final StreamServerMethodInfo value : values()) {
            Assertions.checkEquals(value, valueOfNullable(value.getMethodId()));
        }
    }

    private final boolean blocking;

    StreamServerMethodInfo(final boolean blocking) {
        this.blocking = blocking;
    }

    public static StreamServerMethodInfo valueOfNullable(final int methodId) {
        switch (methodId) {
        case METHOD_ID_PUT:
            return PUT;
        case METHOD_ID_SUBSCRIBE:
            return SUBSCRIBE;
        case METHOD_ID_UNSUBSCRIBE:
            return UNSUBSCRIBE;
        case METHOD_ID_CREATE:
            return CREATE;
        case METHOD_ID_DELETE:
            return DELETE;
        default:
            return null;
        }
    }

    public static URI parseUri(final IByteBufferProvider message) {
        final String uriStr = StringUtf8Serde.GET.fromBuffer(message);
        final URI uri = URIs.asUri("p://" + uriStr);
        return uri;
    }

    public static String parseTopic(final URI uri) {
        final String topic = uri.getHost();
        return topic;
    }

    public static Map<String, String> parseParameters(final URI uri) {
        final Map<String, String> parameters = QUERY_PARAMS_HOLDER.get();
        if (!parameters.isEmpty()) {
            parameters.clear();
        }
        URIs.splitQuery(uri, parameters);
        return parameters;
    }

    public boolean isBlocking() {
        return blocking;
    }

    public abstract int getMethodId();

    protected IStreamSynchronousEndpointService getService(
            final IStreamSynchronousEndpointServerSessionInternalMethods parent, final int serviceId,
            final IByteBufferProvider message) throws Exception {
        return parent.getService(serviceId);
    }

    public Future<Object> invoke(final IStreamSynchronousEndpointServerSessionInternalMethods parent,
            final String sessionId, final IServiceSynchronousCommand<IByteBufferProvider> request,
            final ISerializingServiceSynchronousCommand<Object> response) {
        final int serviceId = request.getService();
        response.setService(serviceId);
        response.setSequence(request.getSequence());
        try {
            final IStreamSynchronousEndpointService service = getService(parent, serviceId, request.getMessage());
            if (service == null) {
                response.setService(serviceId);
                response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
                response.setSequence(request.getSequence());
                response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                        "service not found: " + serviceId);
                return null;
            }

            final Object result = invoke(parent, service, request.getMessage());
            if (result instanceof Future) {
                if (result != null) {
                    @SuppressWarnings("unchecked")
                    final Future<Object> futureResult = (Future<Object>) result;
                    return new APostProcessingFuture<Object>(futureResult) {
                        @Override
                        protected Object onSuccess(final Object value) throws ExecutionException, InterruptedException {
                            handleResult(response, futureResult.get());
                            return null;
                        }

                        @Override
                        protected ExecutionException onError(final ExecutionException exc) {
                            handleException(sessionId, request, response, exc);
                            return null;
                        }
                    };
                }
            }
            handleResult(response, result);
            return null;
        } catch (final Throwable t) {
            handleException(sessionId, request, response, t);
            return null;
        }
    }

    protected abstract Object invoke(IStreamSynchronousEndpointServerSessionInternalMethods parent,
            IStreamSynchronousEndpointService service, IByteBufferProvider message) throws Exception;

    private void handleResult(final ISerializingServiceSynchronousCommand<Object> response, final Object result) {
        response.setMethod(getMethodId());
        if (result == null) {
            response.setMessageBuffer(EmptyByteBuffer.INSTANCE);
        } else {
            response.setMessageBuffer((ICloseableByteBufferProvider) result);
        }
    }

    private void handleException(final String sessionId, final IServiceSynchronousCommand<IByteBufferProvider> request,
            final ISerializingServiceSynchronousCommand<Object> response, final Throwable t) {
        final boolean shouldRetry = Retries.shouldRetry(t);
        final LoggedRuntimeException loggedException = Err.process(new RemoteExecutionException("sessionId=["
                + sessionId + "], serviceId=[" + request.getService() + "]" + ", methodId=[" + getMethodId() + ":"
                + name() + "], sequence=[" + request.getSequence() + "], shouldRetry=[" + shouldRetry + "]", t));
        if (shouldRetry) {
            response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
        } else {
            response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
        }
        if (ContextProperties.IS_TEST_ENVIRONMENT || Throwables.isDebugStackTraceEnabled()) {
            response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    Throwables.getFullStackTrace(loggedException));
        } else {
            //keep full FQDN of exception types so that string matching can at least be done by clients
            response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    Throwables.concatMessages(loggedException));
        }
    }

}
