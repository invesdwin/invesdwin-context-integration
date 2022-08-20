package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class BlockingDatagramSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final boolean SERVER = true;
    private BlockingDatagramSynchronousChannel channel;
    private IByteBuffer packetBuffer;
    private DatagramPacket packet;
    private DatagramSocket socket;

    public BlockingDatagramSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new BlockingDatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public BlockingDatagramSynchronousReader(final BlockingDatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram reader has to be the server");
        }
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        final byte[] packetBytes = ByteBuffers.allocateByteArray(channel.getSocketSize());
        this.packetBuffer = ByteBuffers.wrap(packetBytes);
        this.packet = new DatagramPacket(packetBytes, packetBytes.length);
        this.socket = channel.getSocket();
    }

    @Override
    public void close() throws IOException {
        packet = null;
        packetBuffer = null;
        socket = null;
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        socket.receive(packet);
        return true;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final int size = packetBuffer.getInt(DatagramSynchronousChannel.SIZE_INDEX);
        final IByteBuffer message = packetBuffer.slice(DatagramSynchronousChannel.MESSAGE_INDEX, size);
        if (ClosedByteBuffer.isClosed(message, 0, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
