package de.invesdwin.context.integration.channel.rpc.base.endpoint;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class DisabledSynchronousEndpointFactory<R, W> implements ISynchronousEndpointFactory<R, W> {

    @SuppressWarnings("rawtypes")
    private static final DisabledSynchronousEndpointFactory INSTANCE = new DisabledSynchronousEndpointFactory<>();

    private DisabledSynchronousEndpointFactory() {}

    @Override
    public ISynchronousEndpoint<R, W> newEndpoint() {
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <_R, _W> DisabledSynchronousEndpointFactory<_R, _W> getInstance() {
        return INSTANCE;
    }

}
