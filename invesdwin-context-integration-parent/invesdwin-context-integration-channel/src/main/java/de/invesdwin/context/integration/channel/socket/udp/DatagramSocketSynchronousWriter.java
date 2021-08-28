package de.invesdwin.context.integration.channel.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.lang.buffer.ClosedByteBuffer;
import de.invesdwin.util.lang.buffer.IByteBuffer;

@NotThreadSafe
public class DatagramSocketSynchronousWriter extends ADatagramSocketSynchronousChannel
        implements ISynchronousWriter<IByteBuffer> {

    private static final double BUFFER_GROWTH_FACTOR = 1.25;

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
    public void write(final IByteBuffer message) throws IOException {
        final int size = message.capacity();
        setSize(size);
        packetBuffer.putBytes(MESSAGE_INDEX, message);
        packet.setLength(MESSAGE_INDEX + size);
        socket.send(packet);
    }

}
