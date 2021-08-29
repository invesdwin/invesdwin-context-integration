package de.invesdwin.context.integration.channel.socket.old.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class OldDatagramSocketSynchronousReader extends AOldDatagramSocketSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    protected IByteBuffer packetBuffer;
    protected DatagramPacket packet;

    public OldDatagramSocketSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, true, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        final byte[] packetBytes = new byte[socketSize];
        this.packetBuffer = ByteBuffers.wrap(packetBytes);
        this.packet = new DatagramPacket(packetBytes, packetBytes.length);
    }

    @Override
    public void close() throws IOException {
        super.close();
        packet = null;
        packetBuffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        socket.receive(packet);
        return true;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final int size = packetBuffer.getInt(SIZE_INDEX);
        final IByteBuffer message = packetBuffer.slice(MESSAGE_INDEX, size);
        if (ClosedByteBuffer.isClosed(message)) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
