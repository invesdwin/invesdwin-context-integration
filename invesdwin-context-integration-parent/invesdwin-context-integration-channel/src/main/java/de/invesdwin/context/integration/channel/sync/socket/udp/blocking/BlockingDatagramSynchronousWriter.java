package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class BlockingDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private BlockingDatagramSynchronousChannel channel;
    private IByteBuffer packetBuffer;
    private IByteBuffer messageBuffer;
    private DatagramPacket packet;
    private DatagramSocket socket;
    private final int socketSize;

    public BlockingDatagramSynchronousWriter(final BlockingDatagramSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        packetBuffer = ByteBuffers.allocateExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(packetBuffer, DatagramSynchronousChannel.MESSAGE_INDEX);
        socket = channel.getSocket();
        if (!channel.isServer()) {
            packet = new DatagramPacket(Bytes.EMPTY_ARRAY, 0, channel.getSocketAddress());
        }
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            if (packet != null) {
                try {
                    writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
                } catch (final Throwable t) {
                    //ignore
                }
            }
            packet = null;
            packetBuffer = null;
            messageBuffer = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
        final int datagramSize = DatagramSynchronousChannel.MESSAGE_INDEX + size;
        if (datagramSize > socketSize) {
            throw new IllegalArgumentException(
                    "Data truncation would occur: datagramSize[" + datagramSize + "] > socketSize[" + socketSize + "]");
        }
        packetBuffer.putInt(DatagramSynchronousChannel.SIZE_INDEX, size);
        if (packet == null) {
            packet = new DatagramPacket(packetBuffer.byteArray(), 0, DatagramSynchronousChannel.MESSAGE_INDEX + size,
                    channel.getOtherSocketAddress());
        } else {
            packet.setData(packetBuffer.byteArray(), 0, DatagramSynchronousChannel.MESSAGE_INDEX + size);
        }
        socket.send(packet);
    }

    @Override
    public boolean writeFlushed() {
        return true;
    }

}
