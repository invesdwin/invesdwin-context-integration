package de.invesdwin.context.integration.channel.stream.client;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointClientSubscription {

    void onPush(int serviceId, String topic, IByteBufferProvider message);

}
