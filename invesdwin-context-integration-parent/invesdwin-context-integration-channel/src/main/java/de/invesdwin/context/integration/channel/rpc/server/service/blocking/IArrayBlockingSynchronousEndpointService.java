package de.invesdwin.context.integration.channel.rpc.server.service.blocking;

import java.io.IOException;

public interface IArrayBlockingSynchronousEndpointService {

    byte[] call(byte[] request) throws IOException;

}
