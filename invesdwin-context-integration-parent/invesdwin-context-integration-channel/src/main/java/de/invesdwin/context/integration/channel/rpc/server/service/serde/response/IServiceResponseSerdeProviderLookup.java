package de.invesdwin.context.integration.channel.rpc.server.service.serde.response;

import java.lang.reflect.Method;

public interface IServiceResponseSerdeProviderLookup {

    IServiceResponseSerdeProvider lookup(Method method);

}
