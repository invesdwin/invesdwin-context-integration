package de.invesdwin.context.integration.channel.rpc.server.service.serde.request;

import java.lang.reflect.Method;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.RemoteFastSerializingSerde;

@Immutable
public final class DefaultServiceRequestSerdeLookup implements IServiceRequestSerdeLookup {

    public static final DefaultServiceRequestSerdeLookup INSTANCE = new DefaultServiceRequestSerdeLookup();

    private DefaultServiceRequestSerdeLookup() {}

    @Override
    public ISerde<Object[]> lookup(final Method method) {
        return RemoteFastSerializingSerde.get();
    }

}
