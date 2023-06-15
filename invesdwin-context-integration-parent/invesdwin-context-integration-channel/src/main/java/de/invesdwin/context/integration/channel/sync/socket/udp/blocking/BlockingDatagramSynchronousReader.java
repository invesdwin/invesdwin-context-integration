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
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BlockingDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final boolean SERVER = true;
    private BlockingDatagramSynchronousChannel channel;
    private IByteBuffer packetBuffer;
    private DatagramPacket packet;
    private DatagramSocket socket;
    private final int socketSize;
    private final int truncatedSize;

    public BlockingDatagramSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new BlockingDatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public BlockingDatagramSynchronousReader(final BlockingDatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram reader has to be the server");
        }
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
        this.truncatedSize = socketSize - DatagramSynchronousChannel.MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        final byte[] packetBytes = ByteBuffers.allocateByteArray(socketSize + 1);
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
    public IByteBufferProvider readMessage() throws IOException {
        final int size = packetBuffer.getInt(DatagramSynchronousChannel.SIZE_INDEX);
        if (size > truncatedSize) {
            close();
            throw FastEOFException.getInstance("data truncation occurred: size[%s] > truncatedSize[%s]", size,
                    truncatedSize);
        }
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
