package de.invesdwin.context.integration.channel.stream.server.session;

import java.util.Map;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointServerSessionInternalMethods {

    IStreamSynchronousEndpointService getService(int serviceId);

    IStreamSynchronousEndpointService getOrCreateService(int serviceId, String topic, Map<String, String> parameters);

    Object put(IByteBufferProvider message);

    Object subscribe(IStreamSynchronousEndpointService service);

    Object unsubscribe(IStreamSynchronousEndpointService service);

    Object delete(IStreamSynchronousEndpointService service);

}
