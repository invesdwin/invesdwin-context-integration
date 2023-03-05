package de.invesdwin.context.integration.channel.sync.disni.passive.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.verbs.RdmaCmId;

@Immutable
public class DisniPassiveRdmaEndpointFactory implements RdmaEndpointFactory<DisniPassiveRdmaEndpoint> {

    private final RdmaEndpointGroup<DisniPassiveRdmaEndpoint> endpointGroup;
    private final int socketSize;

    public DisniPassiveRdmaEndpointFactory(final RdmaEndpointGroup<DisniPassiveRdmaEndpoint> endpointGroup,
            final int socketSize) {
        this.endpointGroup = endpointGroup;
        this.socketSize = socketSize;
    }

    @Override
    public DisniPassiveRdmaEndpoint createEndpoint(final RdmaCmId id, final boolean serverSide) throws IOException {
        return new DisniPassiveRdmaEndpoint(endpointGroup, id, serverSide, socketSize);
    }

}
