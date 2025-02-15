package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.IOException;

import de.invesdwin.context.integration.channel.stream.server.IStreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointSession {

    IStreamSynchronousEndpointServer getServer();

    /**
     * Returns true if the message was pushed fully and thus another message can now be pushed. Returns false if
     * response channel is busy and no more messages should be pushed.
     */
    boolean pushSubscriptionMessage(IStreamSynchronousEndpointService service,
            ISynchronousReader<IByteBufferProvider> reader) throws IOException;

}
