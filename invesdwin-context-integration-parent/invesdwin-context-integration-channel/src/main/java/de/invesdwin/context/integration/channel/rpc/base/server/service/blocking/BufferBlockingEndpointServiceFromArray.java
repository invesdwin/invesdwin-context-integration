package de.invesdwin.context.integration.channel.rpc.base.server.service.blocking;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

/**
 * Needs to be pooled from the outside with client sessions.
 */
@NotThreadSafe
public class BufferBlockingEndpointServiceFromArray implements IBufferBlockingEndpointService {

    private final IArrayBlockingEndpointService service;
    private final UnsafeByteBuffer responseBufferWrapper;

    public BufferBlockingEndpointServiceFromArray(final IArrayBlockingEndpointService service) {
        this.service = service;
        this.responseBufferWrapper = new UnsafeByteBuffer(Bytes.EMPTY_ARRAY);
    }

    @Override
    public ICloseableByteBufferProvider call(final IByteBufferProvider request) throws IOException {
        final byte[] response = service.call(request.asBuffer().asByteArrayCopy());
        responseBufferWrapper.wrap(response);
        return responseBufferWrapper;
    }

}
