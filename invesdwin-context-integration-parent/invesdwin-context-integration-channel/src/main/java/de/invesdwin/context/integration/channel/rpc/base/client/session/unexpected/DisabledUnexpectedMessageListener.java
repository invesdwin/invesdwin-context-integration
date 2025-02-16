package de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public final class DisabledUnexpectedMessageListener implements IUnexpectedMessageListener {

    public static final DisabledUnexpectedMessageListener INSTANCE = new DisabledUnexpectedMessageListener();

    private DisabledUnexpectedMessageListener() {}

    @Override
    public boolean onPushedWithoutRequest(final int serviceId, final int methodId, final int streamSequence,
            final IByteBufferProvider message) {
        //discard
        return false;
    }

    @Override
    public void onUnexpectedResponse(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider message) {}

}
