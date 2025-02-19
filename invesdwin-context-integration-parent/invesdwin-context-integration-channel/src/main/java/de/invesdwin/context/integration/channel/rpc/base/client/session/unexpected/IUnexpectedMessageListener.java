package de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IUnexpectedMessageListener {

    /**
     * This is an unexpected streaming message that was pushed without actively polling for it (yet).
     * 
     * Return true if the message should be stored as a written request for later polling. The message should not be
     * leaked as it is bound to the call, instead a copy should be made if it is to be used later.
     */
    boolean onPushedWithoutRequest(ISynchronousEndpointClientSession session, int serviceId, int methodId,
            int streamSequence, IByteBufferProvider message) throws AbortRequestException;

    /**
     * This is an unexpected response for a non-existent request.
     * 
     * Unexpected responses for requests can not be stored for later polling. The message should not be leaked as it is
     * bound to the call, instead a copy should be made if it is to be used later.
     */
    void onUnexpectedResponse(ISynchronousEndpointClientSession session, int serviceId, int methodId,
            int requestSequence, IByteBufferProvider message) throws AbortRequestException;

}
