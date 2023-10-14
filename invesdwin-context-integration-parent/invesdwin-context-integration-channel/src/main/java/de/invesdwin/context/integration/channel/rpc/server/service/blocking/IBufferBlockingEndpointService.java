package de.invesdwin.context.integration.channel.rpc.server.service.blocking;

import java.io.IOException;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

public interface IBufferBlockingEndpointService {

    ICloseableByteBufferProvider call(IByteBufferProvider request) throws IOException;

}
