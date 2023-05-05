package de.invesdwin.context.integration.channel.rpc.server.service;

import java.lang.reflect.Method;

public interface IServiceResponseSerdeProviderLookup {

    IServiceResponseSerdeProvider lookup(Method method);

}
