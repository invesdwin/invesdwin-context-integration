package de.invesdwin.context.integration.channel.stream.server.session.manager;

import de.invesdwin.context.integration.channel.stream.server.StreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointSession {

    StreamSynchronousEndpointServer getParent();

    /**
     * Returns true if the message was pushed fully and thus another message can now be pushed. Returns false if
     * response channel is busy and no more messages should be pushed.
     */
    boolean pushTopicSubscriptionMessage(IStreamSynchronousEndpointService service,
            ISynchronousReader<IByteBufferProvider> reader);

}
