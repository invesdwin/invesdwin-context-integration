package de.invesdwin.context.integration.channel.rpc.server.service;

import de.invesdwin.util.marshallers.serde.ISerde;

public interface IServiceResponseSerdeProvider {

    ISerde<Object> getSerde(Object[] requestArgs);

}
