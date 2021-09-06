package de.invesdwin.context.integration.channel.async.serde;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

public class SerdeAsynchronousHandler<I, O> implements IAsynchronousHandler<IByteBuffer, IByteBufferWriter> {

    @Override
    public IByteBufferWriter open() {
        return null;
    }

    @Override
    public IByteBufferWriter handle(final IByteBuffer input) {
        return null;
    }

    @Override
    public void close() {
    }

}
