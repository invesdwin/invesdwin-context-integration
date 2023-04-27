package de.invesdwin.context.integration.channel.rpc.service;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.service.command.MutableServiceSynchronousCommand;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
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

    public void invoke(final IServiceSynchronousCommand<Object[]> request,
            final MutableServiceSynchronousCommand<Object> response) {
        final Object result = invoke(request.getMethod(), request.getMessage());
        response.setService(request.getService());
        response.setMethod(request.getMethod());
        response.setSequence(request.getSequence());
        response.setMessage(result);
    }

    private Object invoke(final int methodId, final Object... args) {
        final MethodHandle method = methodId_method.get(methodId);
        if (method == null) {
            throw UnknownArgumentException.newInstance(Integer.class, methodId);
        }
        try {
            final Object result = method.invoke(serviceImplementation, args);
            return result;
        } catch (final Throwable e) {
            throw Throwables.propagate(e);
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
