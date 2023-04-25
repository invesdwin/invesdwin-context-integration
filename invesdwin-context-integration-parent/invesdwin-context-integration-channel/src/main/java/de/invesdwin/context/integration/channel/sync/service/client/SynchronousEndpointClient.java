package de.invesdwin.context.integration.channel.sync.service.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.IdentityHashMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.service.client.pool.SynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.sync.service.client.pool.SynchronousEndpointClientSessionPool;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@ThreadSafe
public final class SynchronousEndpointClient implements InvocationHandler {

    private final Class<?> serviceInterface;
    private final int serviceId;
    private final SynchronousEndpointClientSessionPool sessionPool;
    private final ISerde<Object[]> requestSerde;
    private final ISerde<Object> responseSerde;
    private final IdentityHashMap<Method, Integer> method_methodId;

    private SynchronousEndpointClient(final SynchronousEndpointClientSessionPool sessionPool,
            final ISerde<Object[]> requestSerde, final ISerde<Object> responseSerde, final Class<?> serviceInterface) {
        this.serviceInterface = serviceInterface;
        this.serviceId = SynchronousEndpointService.newServiceId(serviceInterface);
        this.sessionPool = sessionPool;
        this.requestSerde = requestSerde;
        this.responseSerde = responseSerde;
        final Method[] methods = Reflections.getUniqueDeclaredMethods(serviceInterface);
        this.method_methodId = new IdentityHashMap<>(methods.length);
        Arrays.sort(methods, Reflections.METHOD_COMPARATOR);
        int ignoredMethods = 0;
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
            if (indexOf < 0) {
                final int methodId = i - ignoredMethods;
                method_methodId.put(method, methodId);
            } else {
                ignoredMethods++;
            }
        }
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final Integer methodId = method_methodId.get(method);
        if (methodId == null) {
            throw UnknownArgumentException.newInstance(Method.class, method);
        }

        try (ICloseableByteBuffer buffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            final int argsSize = requestSerde.toBuffer(buffer, args);
            try (ICloseableByteBufferProvider response = request(methodId, buffer.sliceTo(argsSize))) {
                final Object result = responseSerde.fromBuffer(response);
                return result;
            }
        }
    }

    private ICloseableByteBufferProvider request(final int method, final IByteBufferProvider request) {
        final SynchronousEndpointClientSession session = sessionPool.borrowObject();
        try {
            return session.request(serviceId, method, request);
        } catch (final Throwable t) {
            sessionPool.invalidateObject(session);
            throw Throwables.propagate(t);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(final SynchronousEndpointClientSessionPool sessionPool,
            final ISerde<Object[]> requestSerde, final ISerde<Object> responseSerde, final Class<T> serviceInterface) {
        final SynchronousEndpointClient handler = new SynchronousEndpointClient(sessionPool, requestSerde,
                responseSerde, serviceInterface);
        final T service = (T) Proxy.newProxyInstance(serviceInterface.getClassLoader(),
                new Class[] { serviceInterface }, handler);
        return service;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("interfaceTypeId", serviceId)
                .add("interfaceType", serviceInterface.getName())
                .toString();
    }

}
