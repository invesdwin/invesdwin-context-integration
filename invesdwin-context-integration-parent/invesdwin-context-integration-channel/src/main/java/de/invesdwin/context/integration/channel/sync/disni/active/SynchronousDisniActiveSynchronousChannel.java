package de.invesdwin.context.integration.channel.sync.disni.active;

import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaActiveEndpointGroup;

import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.ADisniActiveRdmaEndpointFactory;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.SynchronousDisniActiveRdmaEndpoint;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.SynchronousDisniActiveRdmaEndpointFactory;

@NotThreadSafe
public class SynchronousDisniActiveSynchronousChannel
        extends ADisniActiveSynchronousChannel<SynchronousDisniActiveRdmaEndpoint> {

    public SynchronousDisniActiveSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    protected ADisniActiveRdmaEndpointFactory<SynchronousDisniActiveRdmaEndpoint> newRdmaEndpointFactory(
            final RdmaActiveEndpointGroup<SynchronousDisniActiveRdmaEndpoint> endpointGroup, final int socketSize) {
        return new SynchronousDisniActiveRdmaEndpointFactory(endpointGroup, socketSize);
    }

}
