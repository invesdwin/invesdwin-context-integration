package de.invesdwin.context.integration.channel.rpc.base.server.service;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.rpc.base.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.base.client.handler.IServiceMethodInfo;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.retry.Retries;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.log.error.LoggedRuntimeException;
import de.invesdwin.util.concurrent.future.APostProcessingFuture;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import de.invesdwin.util.marshallers.serde.lookup.response.IResponseSerdeProvider;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class RpcServerMethodInfo implements IServiceMethodInfo {

    private final RpcSynchronousEndpointService service;
    private final int methodId;
    private final Method method;
    private final MethodHandle methodHandle;
    private boolean reflectionFallback;
    private final ISerde<Object[]> requestSerde;
    private final IResponseSerdeProvider responseSerdeProvider;
    private final boolean blocking;
    private final boolean future;

    RpcServerMethodInfo(final RpcSynchronousEndpointService service, final int methodId, final Method method,
            final Lookup mhLookup, final SerdeLookupConfig serdeLookupConfig) throws IllegalAccessException {
        this.service = service;
        this.methodId = methodId;
        this.method = method;
        this.methodHandle = mhLookup.unreflect(method);
        this.requestSerde = serdeLookupConfig.getRequestLookup().lookup(method);
        this.responseSerdeProvider = serdeLookupConfig.getResponseLookup().lookup(method);
        this.blocking = RpcSynchronousEndpointService.isBlocking(method, false);
        this.future = Future.class.isAssignableFrom(method.getReturnType());
    }

    public RpcSynchronousEndpointService getService() {
        return service;
    }

    @Override
    public int getServiceId() {
        return service.getServiceId();
    }

    @Override
    public int getMethodId() {
        return methodId;
    }

    public Method getMethod() {
        return method;
    }

    public MethodHandle getMethodHandle() {
        return methodHandle;
    }

    public boolean isBlocking() {
        return blocking;
    }

    public boolean isFuture() {
        return future;
    }

    @SuppressWarnings("unchecked")
    public Future<Object> invoke(final String sessionId, final IServiceSynchronousCommand<IByteBufferProvider> request,
            final ISerializingServiceSynchronousCommand<Object> response) {
        final int serviceId = request.getService();
        final int requestSequence = request.getSequence();
        try {
            final Object[] args = requestSerde.fromBuffer(request.getMessage());
            final Object result = invoke(service.getServiceImplementation(), args);
            if (isFuture()) {
                if (result != null) {
                    final Future<Object> futureResult = (Future<Object>) result;
                    return new APostProcessingFuture<Object>(futureResult) {
                        @Override
                        protected Object onSuccess(final Object value) throws ExecutionException, InterruptedException {
                            handleResult(serviceId, requestSequence, response, args, futureResult.get());
                            return null;
                        }

                        @Override
                        protected ExecutionException onError(final ExecutionException exc) {
                            handleException(serviceId, requestSequence, request, response, exc, sessionId);
                            return null;
                        }
                    };
                }
            }
            handleResult(serviceId, requestSequence, response, args, result);
            return null;
        } catch (final Throwable t) {
            handleException(serviceId, requestSequence, request, response, t, sessionId);
            return null;
        }
    }

    private void handleResult(final int serviceId, final int requestSequence,
            final ISerializingServiceSynchronousCommand<Object> response, final Object[] args, final Object result) {
        response.setService(serviceId);
        response.setSequence(requestSequence);
        response.setMethod(methodId);
        final ISerde<Object> resultSerde = responseSerdeProvider.getSerde(args);
        response.setMessage(resultSerde, result);
    }

    private void handleException(final int serviceId, final int requestSequence,
            final IServiceSynchronousCommand<IByteBufferProvider> request,
            final ISerializingServiceSynchronousCommand<Object> response, final Throwable t, final String sessionId) {
        response.setService(serviceId);
        response.setSequence(requestSequence);
        final boolean shouldRetry = Retries.shouldRetry(t);
        final LoggedRuntimeException loggedException = Err.process(
                new RemoteExecutionException("sessionId=[" + sessionId + "], serviceId=[" + request.getService() + ":"
                        + service.getServiceInterface().getName() + "]" + ", methodId=[" + methodId + ":" + methodHandle
                        + "], sequence=[" + request.getSequence() + "], shouldRetry=[" + shouldRetry + "]", t));
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

    private Object invoke(final Object targetObject, final Object... params) throws Throwable {
        if (reflectionFallback) {
            return method.invoke(targetObject, params);
        } else {
            if (params == null) {
                return methodHandle.invoke(targetObject);
            }
            try {
                switch (params.length) {
                case 0:
                    return methodHandle.invoke(targetObject);
                case 1:
                    return methodHandle.invoke(targetObject, params[0]);
                case 2:
                    return methodHandle.invoke(targetObject, params[0], params[1]);
                case 3:
                    return methodHandle.invoke(targetObject, params[0], params[1], params[2]);
                default:
                    final Object[] args = new Object[params.length + 1];
                    System.arraycopy(params, 0, args, 1, params.length);
                    args[0] = targetObject;
                    return methodHandle.invokeWithArguments(args);
                }
            } catch (final ClassCastException t) {
                reflectionFallback = true;
                return method.invoke(targetObject, params);
            }
        }
    }

}