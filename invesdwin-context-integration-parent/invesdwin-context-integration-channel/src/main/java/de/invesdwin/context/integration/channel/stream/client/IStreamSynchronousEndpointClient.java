package de.invesdwin.context.integration.channel.stream.client;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

public interface IStreamSynchronousEndpointClient extends ISynchronousChannel {

    default int newServiceId(final String topic) {
        return RpcSynchronousEndpointService.newServiceId(topic);
    }

    void poll(Duration timeout) throws TimeoutException;

    Future<?> put(int serviceId, ICloseableByteBufferProvider message);

    default Future<?> create(final String topic, final IProperties parameters) {
        final int serviceId = newServiceId(topic);
        final String topicUri = URIs.maybeAddQuery(topic, parameters.asMap());
        return create(serviceId, topicUri);
    }

    Future<?> create(int serviceId, String topicUri);

    default Future<?> subscribe(final String topic, final IProperties parameters,
            final IStreamSynchronousEndpointClientSubscription subscription) {
        final int serviceId = newServiceId(topic);
        final String topicUri = URIs.maybeAddQuery(topic, parameters.asMap());
        return subscribe(serviceId, topicUri, subscription);
    }

    Future<?> subscribe(int serviceId, String topicUri, IStreamSynchronousEndpointClientSubscription subscription);

    default Future<?> unsubscribe(final String topic, final IProperties parameters) {
        final int serviceId = newServiceId(topic);
        final String topicUri = URIs.maybeAddQuery(topic, parameters.asMap());
        return unsubscribe(serviceId, topicUri);
    }

    Future<?> unsubscribe(int serviceId, String topicUri);

    default Future<?> delete(final String topic, final IProperties parameters) {
        final int serviceId = newServiceId(topic);
        final String topicUri = URIs.maybeAddQuery(topic, parameters.asMap());
        return delete(serviceId, topicUri);
    }

    Future<?> delete(int serviceId, String topicUri);

}
