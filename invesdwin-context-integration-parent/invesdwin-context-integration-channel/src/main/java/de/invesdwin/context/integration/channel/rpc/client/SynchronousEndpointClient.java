package de.invesdwin.context.integration.channel.rpc.client;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.server.service.Fast;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import de.invesdwin.util.marshallers.serde.lookup.response.IResponseSerdeProvider;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@ThreadSafe
public final class SynchronousEndpointClient<T> implements Closeable {

    private final Class<T> serviceInterface;
    private final Handler handler;
    private final T service;

    private SynchronousEndpointClient(final Class<T> serviceInterface, final Handler handler, final T service) {
        this.serviceInterface = serviceInterface;
        this.handler = handler;
        this.service = service;
    }

    public ICloseableObjectPool<ISynchronousEndpointClientSession> getSessionPool() {
        return handler.sessionPool;
    }

    public Class<T> getServiceInterface() {
        return serviceInterface;
    }

    public T getService() {
        return service;
    }

    public static <T> SynchronousEndpointClient<T> newInstance(
            final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool,
            final Class<T> serviceInterface) {
        return newInstance(sessionPool, serviceInterface, SerdeLookupConfig.DEFAULT);
    }

    @SuppressWarnings("unchecked")
    public static <T> SynchronousEndpointClient<T> newInstance(
            final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool, final Class<T> serviceInterface,
            final SerdeLookupConfig serdeLookupConfig) {
        final Handler handler = new Handler(sessionPool, serviceInterface, serdeLookupConfig);
        final T service = (T) Proxy.newProxyInstance(serviceInterface.getClassLoader(),
                new Class[] { serviceInterface }, handler);
        final SynchronousEndpointClient<T> client = new SynchronousEndpointClient<T>(serviceInterface, handler,
                service);
        return client;
    }

    @Override
    public void close() {
        getSessionPool().close();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serviceId", handler.serviceId)
                .add("serviceInterface", serviceInterface.getName())
                .toString();
    }

    private static final class Handler implements InvocationHandler {

        private final int serviceId;
        private final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool;
        private final Map<Method, ClientMethodInfo> method_methodInfo;

        private Handler(final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool,
                final Class<?> serviceInterface, final SerdeLookupConfig serdeLookupConfig) {
            this.serviceId = SynchronousEndpointService.newServiceId(serviceInterface);
            this.sessionPool = sessionPool;
            final Method[] methods = Reflections.getUniqueDeclaredMethods(serviceInterface);
            this.method_methodInfo = new HashMap<>(methods.length);
            for (int i = 0; i < methods.length; i++) {
                final Method method = methods[i];
                if (Reflections.getAnnotation(method, Hidden.class) != null) {
                    continue;
                }
                final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
                if (indexOf < 0) {
                    method_methodInfo.putIfAbsent(method, new ClientMethodInfo(this, method, serdeLookupConfig));
                }
            }
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final ClientMethodInfo methodInfo = method_methodInfo.get(method);
            if (methodInfo == null) {
                throw UnknownArgumentException.newInstance(Method.class, method);
            }
            return methodInfo.invoke(args);
        }
    }

    public static final class ClientMethodInfo {

        private final Handler handler;
        private final int methodId;
        private final ISerde<Object[]> requestSerde;
        private final IResponseSerdeProvider responseSerdeProvider;
        private final boolean fast;
        private final boolean future;

        private ClientMethodInfo(final Handler handler, final Method method,
                final SerdeLookupConfig serdeLookupConfig) {
            this.handler = handler;
            this.methodId = SynchronousEndpointService.newMethodId(method);
            this.requestSerde = serdeLookupConfig.getRequestLookup().lookup(method);
            this.responseSerdeProvider = serdeLookupConfig.getResponseLookup().lookup(method);
            this.fast = Reflections.getAnnotation(method, Fast.class) != null
                    || Reflections.getAnnotation(method.getDeclaringClass(), Fast.class) != null;
            this.future = Future.class.isAssignableFrom(method.getReturnType());
        }

        public int getServiceId() {
            return handler.serviceId;
        }

        public int getMethodId() {
            return methodId;
        }

        public boolean isFast() {
            return fast;
        }

        public boolean isFuture() {
            return future;
        }

        public Object invoke(final Object[] args) {
            try (ICloseableByteBuffer buffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
                final int argsSize = requestSerde.toBuffer(buffer, args);
                try (ICloseableByteBufferProvider response = request(buffer.sliceTo(argsSize))) {
                    final ISerde<Object> responseSerde = responseSerdeProvider.getSerde(args);
                    final Object result = responseSerde.fromBuffer(response);
                    return result;
                }
            }
        }

        private ICloseableByteBufferProvider request(final IByteBufferProvider request) {
            final ISynchronousEndpointClientSession session = handler.sessionPool.borrowObject();
            try {
                return session.request(this, request);
            } catch (final Throwable t) {
                handler.sessionPool.invalidateObject(session);
                throw Throwables.propagate(t);
            }
        }

    }

}
