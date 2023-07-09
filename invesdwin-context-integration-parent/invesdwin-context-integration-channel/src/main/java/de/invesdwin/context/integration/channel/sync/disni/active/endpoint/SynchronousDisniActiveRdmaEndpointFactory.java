package de.invesdwin.context.integration.channel.sync.disni.active.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.RdmaCmId;

@Immutable
public class SynchronousDisniActiveRdmaEndpointFactory
        extends ADisniActiveRdmaEndpointFactory<SynchronousDisniActiveRdmaEndpoint> {

    public SynchronousDisniActiveRdmaEndpointFactory(
            final RdmaActiveEndpointGroup<SynchronousDisniActiveRdmaEndpoint> endpointGroup, final int bufferSize) {
        super(endpointGroup, bufferSize);
    }

    @Override
    public SynchronousDisniActiveRdmaEndpoint createEndpoint(final RdmaCmId id, final boolean serverSide)
            throws IOException {
        return new SynchronousDisniActiveRdmaEndpoint(endpointGroup, id, serverSide, bufferSize);
    }

}
