package de.invesdwin.context.integration.channel.sync.disni.active.endpoint;

import javax.annotation.concurrent.Immutable;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;

@Immutable
public abstract class ADisniActiveRdmaEndpointFactory<E extends ADisniActiveRdmaEndpoint>
        implements RdmaEndpointFactory<E> {

    protected final RdmaActiveEndpointGroup<E> endpointGroup;
    protected final int bufferSize;

    public ADisniActiveRdmaEndpointFactory(final RdmaActiveEndpointGroup<E> endpointGroup, final int bufferSize) {
        this.endpointGroup = endpointGroup;
        this.bufferSize = bufferSize;
    }

}
