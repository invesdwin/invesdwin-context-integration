package de.invesdwin.context.integration.channel.ipc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.IdentityHashMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Shorts;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class SynchronousChannelEndpointClient implements InvocationHandler {

    private final Class<?> interfaceType;
    private final int interfaceTypeId;
    private final IObjectPool<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> endpointPool;
    private final ISerde<Object> genericSerde;
    private final Duration requestTimeout;
    private final IdentityHashMap<Method, Short> method_index;

    private SynchronousChannelEndpointClient(final Class<?> interfaceType,
            final IObjectPool<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> endpointPool,
            final ISerde<Object> genericSerde, final Duration requestTimeout) {
        this.interfaceType = interfaceType;
        this.interfaceTypeId = SynchronousChannelEndpointService.newInterfaceTypeId(interfaceType);
        this.endpointPool = endpointPool;
        this.genericSerde = genericSerde;
        this.requestTimeout = requestTimeout;
        final Method[] methods = Reflections.getUniqueDeclaredMethods(interfaceType);
        this.method_index = new IdentityHashMap<>(methods.length);
        Arrays.sort(methods, Reflections.METHOD_COMPARATOR);
        int ignoredMethods = 0;
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
            if (indexOf < 0) {
                final short methodIndex = Shorts.checkedCast(i - ignoredMethods);
                method_index.put(method, methodIndex);
            } else {
                ignoredMethods++;
            }
        }
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final Short methodIndex = method_index.get(method);
        if (methodIndex == null) {
            throw UnknownArgumentException.newInstance(Method.class, method);
        }
        final ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint = endpointPool
                .borrowObject();
        try (ICloseableByteBuffer buffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            buffer.putInt(SynchronousChannelEndpointService.TYPE_INDEX, interfaceTypeId);
            buffer.putShort(SynchronousChannelEndpointService.METHOD_INDEX, methodIndex);
            final int argsSize = genericSerde.toBuffer(buffer.sliceFrom(SynchronousChannelEndpointService.ARGS_INDEX),
                    args);
            buffer.putInt(SynchronousChannelEndpointService.ARGSSIZE_INDEX, argsSize);
            endpoint.getWriterSpinWait()
                    .waitForWrite(buffer.sliceTo(SynchronousChannelEndpointService.ARGS_INDEX + argsSize),
                            requestTimeout);

            final int responseSize = endpoint.getReaderSpinWait().waitForRead(requestTimeout).getBuffer(buffer);
            final Object response = genericSerde.fromBuffer(buffer.sliceTo(responseSize));
            return response;
        } finally {
            endpointPool.returnObject(endpoint);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(
            final IObjectPool<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> endpointPool,
            final ISerde<Object> genericSerde, final Duration requestTimeout, final Class<T> interfaceType) {
        final SynchronousChannelEndpointClient handler = new SynchronousChannelEndpointClient(interfaceType,
                endpointPool, genericSerde, requestTimeout);
        final T service = (T) Proxy.newProxyInstance(interfaceType.getClassLoader(), new Class[] { interfaceType },
                handler);
        return service;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("interfaceTypeId", interfaceTypeId)
                .add("interfaceType", interfaceType.getName())
                .toString();
    }

}
