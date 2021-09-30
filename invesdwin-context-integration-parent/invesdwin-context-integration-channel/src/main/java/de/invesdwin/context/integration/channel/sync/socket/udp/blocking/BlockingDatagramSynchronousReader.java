package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class BlockingDatagramSynchronousReader extends ABlockingDatagramSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    protected IByteBuffer packetBuffer;
    protected DatagramPacket packet;

    public BlockingDatagramSynchronousReader(final SocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        super(socketAddress, true, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        final byte[] packetBytes = ByteBuffers.allocateByteArray(socketSize);
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
        if (ClosedByteBuffer.isClosed(message, 0, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
