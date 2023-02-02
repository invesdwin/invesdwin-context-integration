package de.invesdwin.context.integration.channel.sync.jnetrobust;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class JnetrobustSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final JnetrobustSynchronousChannel channel;
    private IByteBuffer buffer;

    public JnetrobustSynchronousWriter(final JnetrobustSynchronousChannel channel) {
        this.channel = channel;
        channel.registerWriter();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        buffer = ByteBuffers.allocateDirectExpandable();
    }

    @Override
    public void close() throws IOException {
        if (channel.getProtocolHandle() != null) {
            try {
                writeAndFinishIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
        }
        channel.close();
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(buffer);
        channel.getProtocolHandle().send(buffer.asByteArray(0, size));
        if (!channel.isReaderRegistered()) {
            try {
                //receive acks
                channel.getProtocolHandle().receive();
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean writeFinished() throws IOException {
        return true;
    }

}
