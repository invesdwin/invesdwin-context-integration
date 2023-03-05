package de.invesdwin.context.integration.channel.sync.disni.active.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

@Immutable
public class DisniActiveRdmaEndpointFactory implements RdmaEndpointFactory<DisniActiveRdmaEndpoint> {

    private final RdmaActiveEndpointGroup<DisniActiveRdmaEndpoint> endpointGroup;
    private final int bufferSize;

    public DisniActiveRdmaEndpointFactory(final RdmaActiveEndpointGroup<DisniActiveRdmaEndpoint> endpointGroup,
            final int bufferSize) {
        this.endpointGroup = endpointGroup;
        this.bufferSize = bufferSize;
    }

    @Override
    public DisniActiveRdmaEndpoint createEndpoint(final RdmaCmId id, final boolean serverSide) throws IOException {
        return new DisniActiveRdmaEndpoint(endpointGroup, id, serverSide, bufferSize);
    }

}
