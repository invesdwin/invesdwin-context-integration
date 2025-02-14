package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.IOException;
import java.util.Map;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSessionManager {

    IStreamSynchronousEndpointSession getSession();

    IStreamSynchronousEndpointService getService(int serviceId);

    IStreamSynchronousEndpointService getOrCreateService(int serviceId, String topic, Map<String, String> parameters);

    boolean handle() throws IOException;

    /**
     * return true here to indicate that a future result is returned
     */
    boolean isAlwaysFuturePut();

    /**
     * The manager has full control over putting messages into a topic storage of a service. The implementation can
     * decide if a back-off exception should be thrown or if the message can be put into a queue for flushing to disk in
     * its own asynchronous handler. Or if the message can be put fast enough into the topic storage with a blocking
     * call.
     * 
     * The handler has to know that the message is attached to the callers stack and should not be retained. Instead a
     * copy has to be created if to be put into a queue.
     * 
     * The handler also has to know that put requests can come from multiple IO/worker threads, thus the put operation
     * should be thread safe.
     * 
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object put(IStreamSynchronousEndpointService service, IByteBufferProvider message) throws Exception;

    /**
     * return true here to indicate that a future result is returned
     */
    boolean isAlwaysFutureSubscribe();

    /**
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object subscribe(IStreamSynchronousEndpointService service, Map<String, String> parameters) throws Exception;

    /**
     * return true here to indicate that a future result is returned
     */
    boolean isAlwaysFutureUnsubscribe();

    /**
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object unsubscribe(IStreamSynchronousEndpointService service, Map<String, String> parameters) throws Exception;

    /**
     * return true here to indicate that a future result is returned
     */
    boolean isAlwaysFutureDelete();

    /**
     * This method can either return null, a IClosingByteBufferProvider or a Future that gives the actual return value
     * at a later time.
     */
    Object delete(IStreamSynchronousEndpointService service, Map<String, String> parameters) throws Exception;

}
