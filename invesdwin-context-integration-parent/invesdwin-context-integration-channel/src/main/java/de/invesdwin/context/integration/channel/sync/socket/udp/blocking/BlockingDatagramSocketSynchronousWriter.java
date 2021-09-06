package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class BlockingDatagramSocketSynchronousWriter extends ABlockingDatagramSocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    protected IByteBuffer packetBuffer;
    protected IByteBuffer messageBuffer;
    protected DatagramPacket packet;

    public BlockingDatagramSocketSynchronousWriter(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, false, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        packetBuffer = ByteBuffers.allocateExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(packetBuffer, MESSAGE_INDEX);
        packet = new DatagramPacket(Bytes.EMPTY_ARRAY, 0);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            packet = null;
            packetBuffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        final int size = message.write(messageBuffer);
        packetBuffer.putInt(SIZE_INDEX, size);
        packet.setData(packetBuffer.byteArray(), 0, MESSAGE_INDEX + size);
        socket.send(packet);
    }

}
