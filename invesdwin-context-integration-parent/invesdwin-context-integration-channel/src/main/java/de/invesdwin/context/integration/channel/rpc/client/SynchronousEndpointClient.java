package de.invesdwin.context.integration.channel.rpc.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.IdentityHashMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.ClosedSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.client.session.SynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.server.service.IServiceRequestSerdeLookup;
import de.invesdwin.context.integration.channel.rpc.server.service.IServiceResponseSerdeProvider;
import de.invesdwin.context.integration.channel.rpc.server.service.IServiceResponseSerdeProviderLookup;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.concurrent.pool.IObjectPool;
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
public final class SynchronousEndpointClient<T> implements Closeable {

    private final Handler handler;
    private final T service;

    private SynchronousEndpointClient(final Handler handler, final T service) {
        this.handler = handler;
        this.service = service;
    }

    public T getService() {
        return service;
    }

    @SuppressWarnings("unchecked")
    public static <T> SynchronousEndpointClient<T> newInstance(
            final IObjectPool<SynchronousEndpointClientSession> sessionPool,
            final IServiceRequestSerdeLookup requestSerdeProvider,
            final IServiceResponseSerdeProviderLookup responseSerdeProviderLookup, final Class<T> serviceInterface) {
        final Handler handler = new Handler(sessionPool, requestSerdeProvider, responseSerdeProviderLookup,
                serviceInterface);
        final T service = (T) Proxy.newProxyInstance(serviceInterface.getClassLoader(),
                new Class[] { serviceInterface }, handler);
        final SynchronousEndpointClient<T> client = new SynchronousEndpointClient<T>(handler, service);
        return client;
    }

    @Override
    public void close() throws IOException {
        final IObjectPool<SynchronousEndpointClientSession> sessionPoolCopy = handler.sessionPool;
        sessionPoolCopy.clear();
        handler.sessionPool = ClosedSynchronousEndpointClientSessionPool.INSTANCE;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serviceId", handler.serviceId)
                .add("serviceInterface", handler.serviceInterface.getName())
                .toString();
    }

    private static final class Handler implements InvocationHandler {

        private final Class<?> serviceInterface;
        private final int serviceId;
        private IObjectPool<SynchronousEndpointClientSession> sessionPool;
        private final IdentityHashMap<Method, MethodInfo> method_methodInfo;

        private Handler(final IObjectPool<SynchronousEndpointClientSession> sessionPool,
                final IServiceRequestSerdeLookup requestSerdeLookup,
                final IServiceResponseSerdeProviderLookup responseSerdeProviderLookup,
                final Class<?> serviceInterface) {
            this.serviceInterface = serviceInterface;
            this.serviceId = SynchronousEndpointService.newServiceId(serviceInterface);
            this.sessionPool = sessionPool;
            final Method[] methods = Reflections.getUniqueDeclaredMethods(serviceInterface);
            this.method_methodInfo = new IdentityHashMap<>(methods.length);
            for (int i = 0; i < methods.length; i++) {
                final Method method = methods[i];
                if (Reflections.getAnnotation(method, Hidden.class) != null) {
                    continue;
                }
                final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
                if (indexOf < 0) {
                    method_methodInfo.putIfAbsent(method,
                            new MethodInfo(method, requestSerdeLookup, responseSerdeProviderLookup));
                }
            }
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final MethodInfo methodInfo = method_methodInfo.get(method);
            if (methodInfo == null) {
                throw UnknownArgumentException.newInstance(Method.class, method);
            }

            try (ICloseableByteBuffer buffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
                final int argsSize = methodInfo.requestSerde.toBuffer(buffer, args);
                try (ICloseableByteBufferProvider response = request(methodInfo.methodId, buffer.sliceTo(argsSize))) {
                    final ISerde<Object> responseSerde = methodInfo.responseSerdeProvider.getSerde(args);
                    final Object result = responseSerde.fromBuffer(response);
                    return result;
                }
            }
        }

        private ICloseableByteBufferProvider request(final int methodId, final IByteBufferProvider request) {
            final SynchronousEndpointClientSession session = sessionPool.borrowObject();
            try {
                return session.request(serviceId, methodId, request);
            } catch (final Throwable t) {
                sessionPool.invalidateObject(session);
                throw Throwables.propagate(t);
            }
        }
    }

    private static final class MethodInfo {

        private final int methodId;
        private final ISerde<Object[]> requestSerde;
        private final IServiceResponseSerdeProvider responseSerdeProvider;

        private MethodInfo(final Method method, final IServiceRequestSerdeLookup requestSerdeLookup,
                final IServiceResponseSerdeProviderLookup responseSerdeProviderLookup) {
            this.methodId = SynchronousEndpointService.newMethodId(method);
            this.requestSerde = requestSerdeLookup.lookup(method);
            this.responseSerdeProvider = responseSerdeProviderLookup.lookup(method);
        }

    }

}
