package de.invesdwin.context.integration.channel.rpc.base.server.service;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public final class RpcSynchronousEndpointService {

    private final Class<?> serviceInterface;
    private final int serviceId;
    private final Object serviceImplementation;
    private final Int2ObjectMap<RpcServerMethodInfo> methodId_methodInfo;

    private RpcSynchronousEndpointService(final SerdeLookupConfig serdeLookupConfig, final Class<?> serviceInterface,
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
                    final RpcServerMethodInfo existing = methodId_methodInfo.put(methodId,
                            new RpcServerMethodInfo(this, methodId, method, lookup, serdeLookupConfig));
                    if (existing != null) {
                        throw new IllegalStateException(
                                "Already registered [" + method + "] as [" + existing.getMethodHandle() + "]");
                    }
                } catch (final IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static int newServiceId(final Class<?> serviceInterface) {
        final String name = serviceInterface.getName();
        return newServiceId(name);
    }

    public static int newServiceId(final String name) {
        final int hash = name.hashCode();
        if (hash == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
            //prevent collisions
            return -hash;
        } else {
            return hash;
        }
    }

    public static int newMethodId(final Method serviceMethod) {
        final String name = serviceMethod.toString();
        return newMethodId(name);
    }

    public static int newMethodId(final String name) {
        final int hash = name.hashCode();
        if (hash == IServiceSynchronousCommand.ERROR_METHOD_ID
                || hash == IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID) {
            //prevent collisions
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

    public Object getServiceImplementation() {
        return serviceImplementation;
    }

    public RpcServerMethodInfo getMethodInfo(final int methodId) {
        final RpcServerMethodInfo methodInfo = methodId_methodInfo.get(methodId);
        return methodInfo;
    }

    public static <T> RpcSynchronousEndpointService newInstance(final SerdeLookupConfig serdeLookupConfig,
            final Class<? super T> interfaceType, final T implementation) {
        return new RpcSynchronousEndpointService(serdeLookupConfig, interfaceType, implementation);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serviceId", serviceId)
                .add("serviceInterface", serviceInterface.getName())
                .toString();
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
