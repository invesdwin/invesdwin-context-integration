package de.invesdwin.context.integration.channel.rpc.server.service.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.server.service.serde.request.DefaultServiceRequestSerdeLookup;
import de.invesdwin.context.integration.channel.rpc.server.service.serde.request.IServiceRequestSerdeLookup;
import de.invesdwin.context.integration.channel.rpc.server.service.serde.response.DefaultServiceResponseSerdeProviderLookup;
import de.invesdwin.context.integration.channel.rpc.server.service.serde.response.IServiceResponseSerdeProviderLookup;

@Immutable
public class ServiceSerdeLookupConfig {

    public static final ServiceSerdeLookupConfig DEFAULT = new ServiceSerdeLookupConfig(
            DefaultServiceRequestSerdeLookup.INSTANCE, DefaultServiceResponseSerdeProviderLookup.INSTANCE);

    private final IServiceRequestSerdeLookup requestLookup;
    private final IServiceResponseSerdeProviderLookup responseLookup;

    public ServiceSerdeLookupConfig(final IServiceRequestSerdeLookup requestLookup,
            final IServiceResponseSerdeProviderLookup responseLookup) {
        this.requestLookup = requestLookup;
        this.responseLookup = responseLookup;
    }

    public IServiceRequestSerdeLookup getRequestLookup() {
        return requestLookup;
    }

    public IServiceResponseSerdeProviderLookup getResponseLookup() {
        return responseLookup;
    }

}
