package de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public final class PollingLaterUnexpectedMessageListener implements IUnexpectedMessageListener {

    public static final PollingLaterUnexpectedMessageListener INSTANCE = new PollingLaterUnexpectedMessageListener();

    private PollingLaterUnexpectedMessageListener() {}

    @Override
    public boolean onPushedWithoutRequest(final ISynchronousEndpointClientSession session, final int serviceId,
            final int methodId, final int streamSequence, final IByteBufferProvider message) {
        //remember for later polling
        return true;
    }

    @Override
    public void onUnexpectedResponse(final ISynchronousEndpointClientSession session, final int serviceId,
            final int methodId, final int requestSequence, final IByteBufferProvider message) {}

}
