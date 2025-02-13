package de.invesdwin.context.integration.channel.rpc.base.client.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.client.ISynchronousEndpointClient;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.ImmutableFuture;
import de.invesdwin.util.concurrent.future.ThrowableFuture;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public final class RpcSynchronousEndpointClientHandler implements InvocationHandler {

    private final int serviceId;
    private final ISynchronousEndpointClient<?> client;
    private final Map<Method, RpcClientMethodInfo> method_methodInfo;

    public RpcSynchronousEndpointClientHandler(final ISynchronousEndpointClient<?> client) {
        this.client = client;
        this.serviceId = RpcSynchronousEndpointService.newServiceId(client.getServiceInterface());
        final Method[] methods = Reflections.getUniqueDeclaredMethods(client.getServiceInterface());
        this.method_methodInfo = new HashMap<>(methods.length);
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            if (Reflections.getAnnotation(method, Hidden.class) != null) {
                continue;
            }
            final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
            if (indexOf < 0) {
                method_methodInfo.putIfAbsent(method,
                        new RpcClientMethodInfo(this, method, client.getSerdeLookupConfig()));
            }
        }
    }

    public int getServiceId() {
        return serviceId;
    }

    public ISynchronousEndpointClient<?> getClient() {
        return client;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final RpcClientMethodInfo methodInfo = method_methodInfo.get(method);
        if (methodInfo == null) {
            throw UnknownArgumentException.newInstance(Method.class, method);
        }
        if (methodInfo.isFuture()) {
            final WrappedExecutorService futureExecutor = client.getFutureExecutor();
            if (futureExecutor == null || methodInfo.isBlocking()) {
                try {
                    return ImmutableFuture.of(methodInfo.invoke(args));
                } catch (final Throwable t) {
                    return ThrowableFuture.of(t);
                }
            } else {
                return futureExecutor.submit(() -> {
                    return methodInfo.invoke(args);
                });
            }
        } else {
            return methodInfo.invoke(args);
        }
    }

}