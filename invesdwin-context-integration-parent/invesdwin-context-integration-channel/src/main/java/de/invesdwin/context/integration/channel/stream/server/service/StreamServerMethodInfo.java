package de.invesdwin.context.integration.channel.stream.server.service;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.rpc.base.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.retry.Retries;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.log.error.LoggedRuntimeException;
import de.invesdwin.context.system.properties.DisabledProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.context.system.properties.MapProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.future.APostProcessingFuture;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.marshallers.serde.basic.StringUtf8Serde;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import io.netty.util.concurrent.FastThreadLocal;

@Immutable
public enum StreamServerMethodInfo {
    /**
     * PUT should be blocking (no work executor) to keep the insertion order intact, service can decide to execute
     * asynchronously
     */
    PUT(true) {
        @Override
        public int getMethodId() {
            return METHOD_ID_PUT;
        }

        @Override
        public boolean isAlwaysFuture(final IStreamSessionManager manager) {
            return manager.isAlwaysFuturePut();
        }

        @Override
        protected Object invoke(final IStreamSessionManager manager, final IStreamSynchronousEndpointService service,
                final boolean future, final IByteBufferProvider message) throws Exception {
            return manager.put(service, message);
        }

    },
    SUBSCRIBE(false) {
        @Override
        public int getMethodId() {
            return METHOD_ID_SUBSCRIBE;
        }

        @Override
        public boolean isAlwaysFuture(final IStreamSessionManager manager) {
            return manager.isAlwaysFutureSubscribe();
        }

        @Override
        protected Object invoke(final IStreamSessionManager manager, final IStreamSynchronousEndpointService service,
                final boolean future, final IByteBufferProvider message) throws Exception {
            final URI uri = parseUri(message.asBuffer());
            final String topic = parseTopic(uri);
            assertServiceTopic(service, topic);
            final IProperties parameters = parseParameters(uri, future);
            return manager.subscribe(service, parameters);
        }
    },
    UNSUBSCRIBE(false) {
        @Override
        public int getMethodId() {
            return METHOD_ID_UNSUBSCRIBE;
        }

        @Override
        public boolean isAlwaysFuture(final IStreamSessionManager manager) {
            return manager.isAlwaysFutureUnsubscribe();
        }

        @Override
        protected Object invoke(final IStreamSessionManager manager, final IStreamSynchronousEndpointService service,
                final boolean future, final IByteBufferProvider message) throws Exception {
            final URI uri = parseUri(message.asBuffer());
            final String topic = parseTopic(uri);
            assertServiceTopic(service, topic);
            final IProperties parameters = parseParameters(uri, future);
            return manager.unsubscribe(service, parameters);
        }
    },
    CREATE(false) {
        @Override
        public int getMethodId() {
            return METHOD_ID_CREATE;
        }

        @Override
        protected IStreamSynchronousEndpointService getService(final IStreamSessionManager manager, final int serviceId,
                final IByteBufferProvider message) throws Exception {
            final URI uri = parseUri(message);
            final String topic = parseTopic(uri);
            final IProperties parameters = parseParameters(uri, false);
            return manager.getOrCreateService(serviceId, topic, parameters);
        }

        @Override
        public boolean isAlwaysFuture(final IStreamSessionManager manager) {
            return false;
        }

        @Override
        protected Object invoke(final IStreamSessionManager manager, final IStreamSynchronousEndpointService service,
                final boolean future, final IByteBufferProvider message) throws Exception {
            return null;
        }
    },
    DELETE(false) {
        @Override
        public int getMethodId() {
            return METHOD_ID_DELETE;
        }

        @Override
        public boolean isAlwaysFuture(final IStreamSessionManager manager) {
            return manager.isAlwaysFutureDelete();
        }

        @Override
        protected Object invoke(final IStreamSessionManager manager, final IStreamSynchronousEndpointService service,
                final boolean future, final IByteBufferProvider message) throws Exception {
            final URI uri = parseUri(message.asBuffer());
            final String topic = parseTopic(uri);
            assertServiceTopic(service, topic);
            final IProperties parameters = parseParameters(uri, future);
            return manager.delete(service, parameters);
        }
    };

    public static final int METHOD_ID_PUT = 0;
    public static final int METHOD_ID_SUBSCRIBE = 1;
    public static final int METHOD_ID_UNSUBSCRIBE = 2;
    public static final int METHOD_ID_CREATE = 3;
    public static final int METHOD_ID_DELETE = 4;
    public static final int METHOD_ID_PUSH = 10;

    private static final FastThreadLocal<IProperties> QUERY_PARAMS_HOLDER = new FastThreadLocal<IProperties>() {
        @Override
        protected IProperties initialValue() throws Exception {
            return new MapProperties();
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

    public static String parseRequest(final IByteBufferProvider message) {
        final String request = StringUtf8Serde.GET.fromBuffer(message);
        return request;
    }

    public static URI parseUri(final IByteBufferProvider message) {
        final String request = parseRequest(message);
        final URI uri = URIs.asUri("p://" + request);
        return uri;
    }

    public static String parseTopic(final URI uri) {
        final String topic = uri.getHost();
        return topic;
    }

    public static IProperties parseParameters(final URI uri, final boolean future) {
        if (uri == null) {
            return DisabledProperties.INSTANCE;
        }
        final IProperties parameters;
        if (future) {
            parameters = new MapProperties();
        } else {
            parameters = QUERY_PARAMS_HOLDER.get();
            if (!parameters.isEmpty()) {
                parameters.clear();
            }
        }
        URIs.splitQuery(uri, parameters.asMap());
        return parameters;
    }

    public boolean isBlocking() {
        return blocking;
    }

    public abstract boolean isAlwaysFuture(IStreamSessionManager manager);

    public abstract int getMethodId();

    protected IStreamSynchronousEndpointService getService(final IStreamSessionManager manager, final int serviceId,
            final IByteBufferProvider message) throws Exception {
        return manager.getService(serviceId);
    }

    public Future<Object> invoke(final IStreamSessionManager manager, final String sessionId,
            final IServiceSynchronousCommand<IByteBufferProvider> request,
            final ISerializingServiceSynchronousCommand<Object> response) {
        final int serviceId = request.getService();
        response.setService(serviceId);
        response.setSequence(request.getSequence());
        try {
            final IStreamSynchronousEndpointService service = getService(manager, serviceId, request.getMessage());
            if (service == null) {
                response.setService(serviceId);
                response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
                response.setSequence(request.getSequence());
                response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                        "service not found: " + serviceId);
                return null;
            }

            final boolean future = isAlwaysFuture(manager);
            final Object result = invoke(manager, service, future, request.getMessage());
            if (result != null) {
                if (future || result instanceof Future) {
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

    protected abstract Object invoke(IStreamSessionManager manager, IStreamSynchronousEndpointService service,
            boolean future, IByteBufferProvider message) throws Exception;

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

    public static void assertServiceTopic(final IStreamSynchronousEndpointService service, final String topic) {
        if (!service.getTopic().equals(topic)) {
            throw new IllegalStateException("serviceId [" + service.getServiceId() + "] topic mismatch: service.topic ["
                    + service.getTopic() + "] != topic [" + topic + "]");
        }
    }

}
