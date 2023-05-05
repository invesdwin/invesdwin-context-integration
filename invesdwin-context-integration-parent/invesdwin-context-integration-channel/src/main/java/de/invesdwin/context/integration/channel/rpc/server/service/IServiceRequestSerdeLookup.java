package de.invesdwin.context.integration.channel.rpc.server.service;

import java.lang.reflect.Method;

import de.invesdwin.util.marshallers.serde.ISerde;

public interface IServiceRequestSerdeLookup {

    ISerde<Object[]> lookup(Method method);

}
