package de.invesdwin.context.integration.channel.sync.disni.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.verbs.RdmaCmId;

@Immutable
public class CloseableRdmaEndpointFactory implements RdmaEndpointFactory<CloseableRdmaEndpoint> {

    private final RdmaEndpointGroup<CloseableRdmaEndpoint> endpointGroup;

    public CloseableRdmaEndpointFactory(final RdmaEndpointGroup<CloseableRdmaEndpoint> endpointGroup) {
        this.endpointGroup = endpointGroup;
    }

    @Override
    public CloseableRdmaEndpoint createEndpoint(final RdmaCmId id, final boolean serverSide) throws IOException {
        return new CloseableRdmaEndpoint(endpointGroup, id, serverSide);
    }

}
