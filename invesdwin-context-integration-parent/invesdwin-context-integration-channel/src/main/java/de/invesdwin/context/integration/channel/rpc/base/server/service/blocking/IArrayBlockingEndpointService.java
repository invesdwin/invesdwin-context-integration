package de.invesdwin.context.integration.channel.rpc.base.server.service.blocking;

import java.io.IOException;

public interface IArrayBlockingEndpointService {

    byte[] call(byte[] request) throws IOException;

}
