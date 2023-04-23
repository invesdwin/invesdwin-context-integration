package de.invesdwin.context.integration.channel.ipc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.IdentityHashMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWaitPool;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWaitPool;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class SynchronousChannelEndpointClient<T> {

    public static final int METHOD_INDEX = 0;
    public static final int METHOD_SIZE = Integer.BYTES;

    public static final int ARGSSIZE_INDEX = METHOD_INDEX + METHOD_SIZE;
    public static final int ARGSSIZE_SIZE = Integer.BYTES;

    public static final int ARGS_INDEX = ARGSSIZE_INDEX + ARGSSIZE_SIZE;

    private final T service;
    private final Handler handler;

    private SynchronousChannelEndpointClient(final T service, final Handler handler) {
        this.service = service;
        this.handler = handler;
    }

    public T getService() {
        return service;
    }

    @SuppressWarnings("unchecked")
    public static <T> SynchronousChannelEndpointClient<T> newInstance(final Class<T> interfaceType,
            final IObjectPool<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> endpointPool,
            final ISerde<Object> genericSerde, final Duration requestTimeout) {
        final Handler handler = new Handler(interfaceType, endpointPool, genericSerde, requestTimeout);
        final T service = (T) Proxy.newProxyInstance(interfaceType.getClassLoader(), new Class[] { interfaceType },
                handler);
        return new SynchronousChannelEndpointClient<T>(service, handler);
    }

    private static final class Handler implements InvocationHandler {

        private final IObjectPool<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> endpointPool;
        private final ISerde<Object> genericSerde;
        private final Duration requestTimeout;
        private final IdentityHashMap<Method, Integer> method_index;

        private Handler(final Class<?> interfaceType,
                final IObjectPool<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> endpointPool,
                final ISerde<Object> genericSerde, final Duration requestTimeout) {
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
                    final int methodIndex = i - ignoredMethods;
                    method_index.put(method, methodIndex);
                } else {
                    ignoredMethods++;
                }
            }
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final Integer methodIndex = method_index.get(method);
            if (methodIndex == null) {
                throw UnknownArgumentException.newInstance(Method.class, method);
            }
            final ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint = endpointPool
                    .borrowObject();
            final SynchronousWriterSpinWait<IByteBufferProvider> writerSpinWait = SynchronousWriterSpinWaitPool
                    .borrowObject(endpoint.getWriter());
            final SynchronousReaderSpinWait<IByteBufferProvider> readerSpinWait = SynchronousReaderSpinWaitPool
                    .borrowObject(endpoint.getReader());
            try (ICloseableByteBuffer buffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
                buffer.putInt(METHOD_INDEX, methodIndex);
                final int argsSize = genericSerde.toBuffer(buffer.sliceFrom(ARGS_INDEX), args);
                buffer.putInt(ARGSSIZE_INDEX, argsSize);
                writerSpinWait.waitForWrite(buffer.sliceTo(ARGS_INDEX + argsSize), requestTimeout);

                final int responseSize = readerSpinWait.waitForRead(requestTimeout).getBuffer(buffer);
                final Object response = genericSerde.fromBuffer(buffer.sliceTo(responseSize));
                return response;
            } finally {
                SynchronousReaderSpinWaitPool.returnObject(readerSpinWait);
                SynchronousWriterSpinWaitPool.returnObject(writerSpinWait);
                endpointPool.returnObject(endpoint);
            }
        }

    }

}
