package de.invesdwin.context.integration.channel.sync.disni.active;

import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaActiveEndpointGroup;

import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.ADisniActiveRdmaEndpointFactory;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.DisniActiveRdmaEndpoint;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.DisniActiveRdmaEndpointFactory;

@NotThreadSafe
public class DisniActiveSynchronousChannel extends ADisniActiveSynchronousChannel<DisniActiveRdmaEndpoint> {

    public DisniActiveSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    DisniActiveSynchronousChannel(final DisniActiveSynchronousChannelServer server,
            final DisniActiveRdmaEndpoint endpoint) {
        super(server, endpoint);
    }

    @Override
    protected ADisniActiveRdmaEndpointFactory<DisniActiveRdmaEndpoint> newRdmaEndpointFactory(
            final RdmaActiveEndpointGroup<DisniActiveRdmaEndpoint> endpointGroup, final int socketSize) {
        return new DisniActiveRdmaEndpointFactory(endpointGroup, socketSize);
    }

}
