package de.invesdwin.context.integration.channel.sync.disni.active.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.RdmaCmId;

@Immutable
public class DisniActiveRdmaEndpointFactory
        extends ADisniActiveRdmaEndpointFactory<DisniActiveRdmaEndpoint> {

    public DisniActiveRdmaEndpointFactory(
            final RdmaActiveEndpointGroup<DisniActiveRdmaEndpoint> endpointGroup, final int bufferSize) {
        super(endpointGroup, bufferSize);
    }

    @Override
    public DisniActiveRdmaEndpoint createEndpoint(final RdmaCmId id, final boolean serverSide)
            throws IOException {
        return new DisniActiveRdmaEndpoint(endpointGroup, id, serverSide, bufferSize);
    }

}
