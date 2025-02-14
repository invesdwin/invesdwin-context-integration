package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.util.Map;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointServerSessionManager {

    IStreamSynchronousEndpointServerSession getSession();

    IStreamSynchronousEndpointService getService(int serviceId);

    IStreamSynchronousEndpointService getOrCreateService(int serviceId, String topic, Map<String, String> parameters);

    boolean handle();

    /**
     * return true here to indicate that a future result is returned
     */
    boolean isFuture(StreamServerMethodInfo streamServerMethodInfo);

    /**
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object put(IStreamSynchronousEndpointService service, IByteBufferProvider message);

    /**
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object subscribe(IStreamSynchronousEndpointService service, Map<String, String> parameters);

    /**
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object unsubscribe(IStreamSynchronousEndpointService service, Map<String, String> parameters);

    /**
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object delete(IStreamSynchronousEndpointService service, Map<String, String> parameters);

}
