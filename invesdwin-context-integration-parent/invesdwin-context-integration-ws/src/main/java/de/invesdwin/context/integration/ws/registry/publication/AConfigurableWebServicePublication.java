package de.invesdwin.context.integration.ws.registry.publication;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class AConfigurableWebServicePublication implements IConfigurableWebServicePublication {

    private boolean useRegistry = true;

    @Override
    public void setUseRegistry(final boolean useRegistry) {
        this.useRegistry = useRegistry;
    }

    @Override
    public boolean isUseRegistry() {
        return useRegistry;
    }

}
