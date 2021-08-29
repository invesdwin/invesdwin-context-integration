package de.invesdwin.context.integration.channel.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class DatagramSocketSynchronousWriter extends ADatagramSocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    public DatagramSocketSynchronousWriter(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, false, estimatedMaxMessageSize);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        final IByteBuffer buffer = message.asByteBuffer();
        final int size = buffer.capacity();
        setSize(size);
        packetBuffer.putBytes(MESSAGE_INDEX, buffer);
        packet.setLength(MESSAGE_INDEX + size);
        socket.send(packet);
    }

}
