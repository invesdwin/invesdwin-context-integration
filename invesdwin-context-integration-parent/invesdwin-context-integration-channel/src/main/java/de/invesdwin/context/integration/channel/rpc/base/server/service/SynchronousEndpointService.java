package de.invesdwin.context.integration.channel.rpc.base.server.service;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.rpc.base.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.retry.Retries;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.log.error.LoggedRuntimeException;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.concurrent.future.APostProcessingFuture;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import de.invesdwin.util.marshallers.serde.lookup.response.IResponseSerdeProvider;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public final class SynchronousEndpointService {

    private final Class<?> serviceInterface;
    private final int serviceId;
    private final Object serviceImplementation;
    private final Int2ObjectMap<ServerMethodInfo> methodId_methodInfo;

    private SynchronousEndpointService(final SerdeLookupConfig serdeLookupConfig, final Class<?> serviceInterface,
            final Object serviceImplementation) {
        this.serviceInterface = serviceInterface;
        this.serviceId = newServiceId(serviceInterface);
        this.serviceImplementation = serviceImplementation;
        final Method[] methods = Reflections.getUniqueDeclaredMethods(serviceInterface);
        /*
         * We sacrifice a bit of speed here by using a hashmap instead of an indexed array in order to not break
         * compatibility when adding methods that change the order of other methods indexes. That way older versions of
         * service interfaces will work with newer versions as long as the indivual methods signatures stay the same.
         */
        this.methodId_methodInfo = new Int2ObjectOpenHashMap<>(methods.length);
        final Lookup lookup = MethodHandles.lookup();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            if (Reflections.getAnnotation(method, Hidden.class) != null) {
                continue;
            }
            final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
            if (indexOf < 0) {
                final int methodId = newMethodId(method);
                try {
                    final ServerMethodInfo existing = methodId_methodInfo.put(methodId,
                            new ServerMethodInfo(this, methodId, method, lookup, serdeLookupConfig));
                    if (existing != null) {
                        throw new IllegalStateException(
                                "Already registered [" + method + "] as [" + existing.methodHandle + "]");
                    }
                } catch (final IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static int newServiceId(final Class<?> serviceInterface) {
        final int hash = serviceInterface.getName().hashCode();
        if (hash == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
            return -hash;
        } else {
            return hash;
        }
    }

    public static int newMethodId(final Method serviceMethod) {
        final int hash = serviceMethod.toString().hashCode();
        if (hash == IServiceSynchronousCommand.ERROR_METHOD_ID
                || hash == IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID) {
            return -hash;
        } else {
            return hash;
        }
    }

    public int getServiceId() {
        return serviceId;
    }

    public Class<?> getServiceInterface() {
        return serviceInterface;
    }

    public ServerMethodInfo getMethodInfo(final int methodId) {
        final ServerMethodInfo methodInfo = methodId_methodInfo.get(methodId);
        return methodInfo;
    }

    public static <T> SynchronousEndpointService newInstance(final SerdeLookupConfig serdeLookupConfig,
            final Class<? super T> interfaceType, final T implementation) {
        return new SynchronousEndpointService(serdeLookupConfig, interfaceType, implementation);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serviceId", serviceId)
                .add("serviceInterface", serviceInterface.getName())
                .toString();
    }

    public static final class ServerMethodInfo {

        private final SynchronousEndpointService service;
        private final int methodId;
        private final MethodHandle methodHandle;
        private final ISerde<Object[]> requestSerde;
        private final IResponseSerdeProvider responseSerdeProvider;
        private final boolean blocking;
        private final boolean future;

        private ServerMethodInfo(final SynchronousEndpointService service, final int methodId, final Method method,
                final Lookup mhLookup, final SerdeLookupConfig serdeLookupConfig) throws IllegalAccessException {
            this.service = service;
            this.methodId = methodId;
            this.methodHandle = mhLookup.unreflect(method);
            this.requestSerde = serdeLookupConfig.getRequestLookup().lookup(method);
            this.responseSerdeProvider = serdeLookupConfig.getResponseLookup().lookup(method);
            this.blocking = SynchronousEndpointService.isBlocking(method, false);
            this.future = Future.class.isAssignableFrom(method.getReturnType());
        }

        public SynchronousEndpointService getService() {
            return service;
        }

        public int getMethodId() {
            return methodId;
        }

        public boolean isBlocking() {
            return blocking;
        }

        public boolean isFuture() {
            return future;
        }

        @SuppressWarnings("unchecked")
        public Future<Object> invoke(final String sessionId,
                final IServiceSynchronousCommand<IByteBufferProvider> request,
                final ISerializingServiceSynchronousCommand<Object> response) {
            response.setService(request.getService());
            response.setSequence(request.getSequence());
            try {
                final Object[] args = requestSerde.fromBuffer(request.getMessage());
                final Object result = invoke(service.serviceImplementation, args);
                if (isFuture()) {
                    if (result != null) {
                        final Future<Object> futureResult = (Future<Object>) result;
                        return new APostProcessingFuture<Object>(futureResult) {
                            @Override
                            protected Object onSuccess(final Object value)
                                    throws ExecutionException, InterruptedException {
                                handleResult(response, args, futureResult.get());
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
                handleResult(response, args, result);
                return null;
            } catch (final Throwable t) {
                handleException(sessionId, request, response, t);
                return null;
            }
        }

        private void handleResult(final ISerializingServiceSynchronousCommand<Object> response, final Object[] args,
                final Object result) {
            response.setMethod(methodId);
            final ISerde<Object> resultSerde = responseSerdeProvider.getSerde(args);
            response.setMessage(resultSerde, result);
        }

        private void handleException(final String sessionId,
                final IServiceSynchronousCommand<IByteBufferProvider> request,
                final ISerializingServiceSynchronousCommand<Object> response, final Throwable t) {
            final boolean shouldRetry = Retries.shouldRetry(t);
            final LoggedRuntimeException loggedException = Err.process(new RemoteExecutionException(
                    "sessionId=[" + sessionId + "], serviceId=[" + request.getService() + ":"
                            + service.serviceInterface.getName() + "]" + ", methodId=[" + methodId + ":" + methodHandle
                            + "], sequence=[" + request.getSequence() + "], shouldRetry=[" + shouldRetry + "]",
                    t));
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
            if (params == null) {
                return methodHandle.invoke(targetObject);
            }
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
        }

    }

    public static boolean isBlocking(final Method method, final boolean client) {
        Blocking blockingAnnotation = Reflections.getAnnotation(method, Blocking.class);
        if (blockingAnnotation == null) {
            blockingAnnotation = Reflections.getAnnotation(method.getDeclaringClass(), Blocking.class);
        }
        if (blockingAnnotation == null) {
            return false;
        }
        if (client) {
            return blockingAnnotation.client();
        } else {
            return blockingAnnotation.server();
        }
    }

}
