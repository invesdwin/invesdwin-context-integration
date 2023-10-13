package de.invesdwin.context.integration.channel.rpc.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.ImmutableFuture;
import de.invesdwin.util.concurrent.future.ThrowableFuture;
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
public final class SynchronousEndpointClient<T> implements ISynchronousEndpointClient<T> {

    public static final int DEFAULT_MAX_PENDING_WORK_COUNT = AsynchronousEndpointServerHandlerFactory.DEFAULT_MAX_PENDING_WORK_COUNT_PER_SESSION;

    public static final WrappedExecutorService DEFAULT_FUTURE_EXECUTOR = Executors
            .newFixedThreadPool(SynchronousEndpointServer.class.getSimpleName() + "_FUTURE",
                    DEFAULT_MAX_PENDING_WORK_COUNT)
            .setDynamicThreadName(false);

    private final Class<T> serviceInterface;
    private final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool;
    private final SerdeLookupConfig serdeLookupConfig;
    private final WrappedExecutorService futureExecutor;
    private final Handler handler;
    private final T service;

    @SuppressWarnings("unchecked")
    public SynchronousEndpointClient(final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool,
            final Class<T> serviceInterface) {
        this.serviceInterface = serviceInterface;
        this.sessionPool = sessionPool;
        this.serdeLookupConfig = newSerdeLookupConfig();
        this.futureExecutor = newFutureExecutor();
        this.handler = new Handler(this);
        this.service = (T) Proxy.newProxyInstance(serviceInterface.getClassLoader(), new Class[] { serviceInterface },
                handler);
    }

    protected SerdeLookupConfig newSerdeLookupConfig() {
        return SerdeLookupConfig.DEFAULT;
    }

    /**
     * Return null here to disable async execution of futures. The calling thread will block for as long as the future
     * is not finished and return an ImmutableFuture once finished.
     */
    protected WrappedExecutorService newFutureExecutor() {
        return DEFAULT_FUTURE_EXECUTOR;
    }

    @Override
    public SerdeLookupConfig getSerdeLookupConfig() {
        return serdeLookupConfig;
    }

    public ICloseableObjectPool<ISynchronousEndpointClientSession> getSessionPool() {
        return sessionPool;
    }

    @Override
    public int getServiceId() {
        return handler.serviceId;
    }

    @Override
    public Class<T> getServiceInterface() {
        return serviceInterface;
    }

    @Override
    public T getService() {
        return service;
    }

    @Override
    public void close() {
        sessionPool.close();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serviceId", getServiceId())
                .add("serviceInterface", serviceInterface.getName())
                .toString();
    }

    private static final class Handler implements InvocationHandler {

        private final int serviceId;
        private final SynchronousEndpointClient<?> client;
        private final Map<Method, ClientMethodInfo> method_methodInfo;

        private Handler(final SynchronousEndpointClient<?> client) {
            this.client = client;
            this.serviceId = SynchronousEndpointService.newServiceId(client.serviceInterface);
            final Method[] methods = Reflections.getUniqueDeclaredMethods(client.serviceInterface);
            this.method_methodInfo = new HashMap<>(methods.length);
            for (int i = 0; i < methods.length; i++) {
                final Method method = methods[i];
                if (Reflections.getAnnotation(method, Hidden.class) != null) {
                    continue;
                }
                final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
                if (indexOf < 0) {
                    method_methodInfo.putIfAbsent(method, new ClientMethodInfo(this, method, client.serdeLookupConfig));
                }
            }
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final ClientMethodInfo methodInfo = method_methodInfo.get(method);
            if (methodInfo == null) {
                throw UnknownArgumentException.newInstance(Method.class, method);
            }
            if (methodInfo.isFuture()) {
                if (client.futureExecutor == null || methodInfo.isBlocking()) {
                    try {
                        return ImmutableFuture.of(methodInfo.invoke(args));
                    } catch (final Throwable t) {
                        return ThrowableFuture.of(t);
                    }
                } else {
                    return client.futureExecutor.submit(() -> {
                        return methodInfo.invoke(args);
                    });
                }
            } else {
                return methodInfo.invoke(args);
            }
        }
    }

    public static final class ClientMethodInfo {

        private final Handler handler;
        private final int methodId;
        private final ISerde<Object[]> requestSerde;
        private final IResponseSerdeProvider responseSerdeProvider;
        private final boolean blocking;
        private final boolean future;

        private ClientMethodInfo(final Handler handler, final Method method,
                final SerdeLookupConfig serdeLookupConfig) {
            this.handler = handler;
            this.methodId = SynchronousEndpointService.newMethodId(method);
            this.requestSerde = serdeLookupConfig.getRequestLookup().lookup(method);
            this.responseSerdeProvider = serdeLookupConfig.getResponseLookup().lookup(method);
            this.blocking = SynchronousEndpointService.isBlocking(method, true);
            this.future = Future.class.isAssignableFrom(method.getReturnType());
        }

        public int getServiceId() {
            return handler.serviceId;
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
            final ISynchronousEndpointClientSession session = handler.client.sessionPool.borrowObject();
            try {
                return session.request(this, request);
            } catch (final Throwable t) {
                handler.client.sessionPool.invalidateObject(session);
                throw Throwables.propagate(t);
            }
        }

    }

}
