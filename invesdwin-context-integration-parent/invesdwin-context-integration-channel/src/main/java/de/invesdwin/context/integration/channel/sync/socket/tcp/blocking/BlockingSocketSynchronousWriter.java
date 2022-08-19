package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class BlockingSocketSynchronousWriter extends ABlockingSocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private OutputStream out;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public BlockingSocketSynchronousWriter(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        socket.shutdownInput();
        out = socket.getOutputStream();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        buffer = ByteBuffers.allocateExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            try {
                out.close();
            } catch (final Throwable t) {
                //ignore
            }
            out = null;
            buffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.writeBuffer(messageBuffer);
            buffer.putInt(SIZE_INDEX, size);
            buffer.getBytesTo(0, out, MESSAGE_INDEX + size);
            out.flush();
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

}
