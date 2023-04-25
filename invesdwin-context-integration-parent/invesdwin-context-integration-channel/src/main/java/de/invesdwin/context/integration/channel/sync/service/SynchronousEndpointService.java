package de.invesdwin.context.integration.channel.sync.service;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.service.command.MutableServiceSynchronousCommand;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;

@ThreadSafe
public final class SynchronousEndpointService {

    private final Class<?> serviceInterface;
    private final int serviceId;
    private final Object serviceImplementation;
    private final List<MethodHandle> methodId_method;

    private SynchronousEndpointService(final Class<?> serviceInterface, final Object serviceImplementation) {
        this.serviceInterface = serviceInterface;
        this.serviceId = newServiceId(serviceInterface);
        this.serviceImplementation = serviceImplementation;
        final Method[] methods = Reflections.getUniqueDeclaredMethods(serviceInterface);
        this.methodId_method = new ArrayList<>(methods.length);
        Arrays.sort(methods, Reflections.METHOD_COMPARATOR);
        int ignoredMethods = 0;
        final Lookup lookup = MethodHandles.lookup();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
            if (indexOf < 0) {
                final int methodId = i - ignoredMethods;
                try {
                    Assertions.checkEquals(methodId, methodId_method.size());
                    methodId_method.add(lookup.unreflect(method));
                } catch (final IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            } else {
                ignoredMethods++;
            }
        }
    }

    public static int newServiceId(final Class<?> serviceInterface) {
        return serviceInterface.getName().hashCode();
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
