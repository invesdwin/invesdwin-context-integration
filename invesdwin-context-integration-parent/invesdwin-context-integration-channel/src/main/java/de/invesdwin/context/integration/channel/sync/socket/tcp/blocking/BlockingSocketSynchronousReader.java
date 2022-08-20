package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class BlockingSocketSynchronousReader implements ISynchronousReader<IByteBuffer> {

    protected BlockingSocketSynchronousChannel channel;
    private InputStream in;
    private IByteBuffer buffer;

    public BlockingSocketSynchronousReader(final BlockingSocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        if (!channel.isWriterRegistered()) {
            channel.getSocket().shutdownOutput();
        }
        in = channel.getSocket().getInputStream();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        buffer = ByteBuffers.allocateExpandable(channel.getSocketSize());
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            buffer = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            final int available = in.available();
            return available >= BlockingSocketSynchronousChannel.MESSAGE_INDEX;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        try {
            buffer.putBytesTo(0, in, BlockingSocketSynchronousChannel.MESSAGE_INDEX);
            final int size = buffer.getInt(BlockingSocketSynchronousChannel.SIZE_INDEX);
            buffer.putBytesTo(0, in, size);
            if (ClosedByteBuffer.isClosed(buffer, 0, size)) {
                close();
                throw FastEOFException.getInstance("closed by other side");
            }
            return buffer.sliceTo(size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public void readFinished() {
        //noop
    }

}
