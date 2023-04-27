package de.invesdwin.context.integration.channel.rpc.service;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.rpc.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.service.command.MutableServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.session.RemoteExecutionException;
import de.invesdwin.context.integration.retry.Retries;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.log.error.LoggedRuntimeException;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public final class SynchronousEndpointService {

    private final Class<?> serviceInterface;
    private final int serviceId;
    private final Object serviceImplementation;
    private final Int2ObjectMap<MethodHandle> methodId_method;

    private SynchronousEndpointService(final Class<?> serviceInterface, final Object serviceImplementation) {
        this.serviceInterface = serviceInterface;
        this.serviceId = newServiceId(serviceInterface);
        this.serviceImplementation = serviceImplementation;
        final Method[] methods = Reflections.getUniqueDeclaredMethods(serviceInterface);
        /*
         * We sacrifice a bit of speed here by using a hashmap instead of an indexed array in order to not break
         * compatibility when adding methods that change the order of other methods indexes. That way older versions of
         * service interfaces will work with newer versions as long as the indivual methods signatures stay the same.
         */
        this.methodId_method = new Int2ObjectOpenHashMap<>(methods.length);
        final Lookup lookup = MethodHandles.lookup();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
            if (indexOf < 0) {
                final int methodId = newMethodId(method);
                try {
                    final MethodHandle methodHandle = lookup.unreflect(method);
                    final MethodHandle existing = methodId_method.put(methodId, methodHandle);
                    if (existing != null) {
                        throw new IllegalStateException(
                                "Already registered [" + methodHandle + "] as [" + existing + "]");
                    }
                } catch (final IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static int newServiceId(final Class<?> serviceInterface) {
        return serviceInterface.getName().hashCode();
    }

    public static int newMethodId(final Method serviceMethod) {
        return serviceMethod.toString().hashCode();
    }

    public int getServiceId() {
        return serviceId;
    }

    public Class<?> getServiceInterface() {
        return serviceInterface;
    }

    public void invoke(final String sessionId, final IServiceSynchronousCommand<Object[]> request,
            final MutableServiceSynchronousCommand<Object> response) {
        response.setService(request.getService());
        response.setSequence(request.getSequence());
        final int methodId = request.getMethod();
        final MethodHandle method = methodId_method.get(methodId);
        try {
            if (method == null) {
                throw new IllegalArgumentException("Unknown methodId: " + methodId);
            }
            final Object[] args = request.getMessage();
            final Object result = method.invoke(serviceImplementation, args);
            response.setMethod(methodId);
            response.setMessage(result);
        } catch (final Throwable t) {
            final boolean shouldRetry = Retries.shouldRetry(t);
            final LoggedRuntimeException loggedException = Err
                    .process(new RemoteExecutionException(
                            "sessionId=[" + sessionId + "], serviceId=[" + request.getService() + ":"
                                    + serviceInterface.getName() + "]" + ", methodId=[" + methodId + ":" + method
                                    + "], sequence=[" + request.getSequence() + "], shouldRetry=[" + shouldRetry + "]",
                            t));
            if (shouldRetry) {
                response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
            } else {
                response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            }
            if (ContextProperties.IS_TEST_ENVIRONMENT || Throwables.isDebugStackTraceEnabled()) {
                response.setMessage(Throwables.getFullStackTrace(loggedException));
            } else {
                //keep full FQDN of exception types so that string matching can at least be done by clients
                response.setMessage(Throwables.concatMessages(loggedException));
            }
        }
    }

    public static <T> SynchronousEndpointService newInstance(final Class<? super T> interfaceType,
            final T implementation) {
        return new SynchronousEndpointService(interfaceType, implementation);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serviceId", serviceId)
                .add("serviceInterface", serviceInterface.getName())
                .toString();
    }

}
