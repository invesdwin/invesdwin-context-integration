package de.invesdwin.context.integration.channel.ipc;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class SynchronousChannelEndpointServer {

    private final ISynchronousReader<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> serverAcceptor;
    private final ISerde<Object> genericSerde;
    private final Duration requestTimeout;
    @GuardedBy("this")
    private final Int2ObjectMap<SynchronousChannelEndpointService> interfaceTypeId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<SynchronousChannelEndpointService> interfaceTypeId_service_copy = new Int2ObjectOpenHashMap<>();

    public SynchronousChannelEndpointServer(
            final ISynchronousReader<ISynchronousChannelEndpoint<IByteBufferProvider, IByteBufferProvider>> serverAcceptor,
            final ISerde<Object> genericSerde, final Duration requestTimeout) {
        this.serverAcceptor = serverAcceptor;
        this.genericSerde = genericSerde;
        this.requestTimeout = requestTimeout;
    }

    public synchronized <T> void register(final Class<? super T> interfaceType, final T implementation) {
        final SynchronousChannelEndpointService service = SynchronousChannelEndpointService.newInstance(interfaceType,
                implementation, genericSerde);
        final SynchronousChannelEndpointService existing = interfaceTypeId_service_sync
                .putIfAbsent(service.getInterfaceTypeId(), service);
        if (existing != null) {
            throw new IllegalStateException("Already registered [" + service + "] as [" + existing + "]");
        }

        //create a new copy of the map so that server thread does not require synchronization
        this.interfaceTypeId_service_copy = new Int2ObjectOpenHashMap<>(interfaceTypeId_service_sync);
    }

    public synchronized <T> boolean unregister(final Class<? super T> interfaceType) {
        final int interfaceTypeId = SynchronousChannelEndpointService.newInterfaceTypeId(interfaceType);
        final SynchronousChannelEndpointService removed = interfaceTypeId_service_sync.remove(interfaceTypeId);
        return removed != null;
    }

}
